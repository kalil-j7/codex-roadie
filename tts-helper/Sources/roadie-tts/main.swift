import AVFoundation
import Foundation
import HuggingFace
import MLX
import MLXAudioCore
import MLXAudioTTS

private let modelIdentifier = "mlx-community/Kokoro-82M-bf16"
private let modelRevision = "a71e4d38b236d968966a2002c4c895dbd12b1c3c"
private let modelVoice = "af_sarah"
private let englishG2PIdentifier = "beshkenadze/kitten-tts-g2p"
private let englishG2PRevision = "9c692b92682d959d9013a9cfe6a49541997add18"

private func preparePinnedEnglishG2P(
    client: HubClient,
    cache: HubCache,
    progressHandler: @escaping @MainActor @Sendable (Progress) -> Void
) async throws {
    guard let repoID = Repo.ID(rawValue: englishG2PIdentifier) else {
        throw URLError(.badURL)
    }
    let directory = cache.cacheDirectory
        .appendingPathComponent("mlx-audio")
        .appendingPathComponent("beshkenadze_kitten-tts-g2p")
    let marker = directory.appendingPathComponent(".roadie-pinned-revision")
    let markerValue = try? String(contentsOf: marker, encoding: .utf8)
    let weights = directory.appendingPathComponent("us_bart.safetensors")
    let dictionary = directory.appendingPathComponent("us_gold.json")
    if markerValue == englishG2PRevision,
       FileManager.default.fileExists(atPath: weights.path),
       FileManager.default.fileExists(atPath: dictionary.path) {
        return
    }

    try? FileManager.default.removeItem(at: directory)
    try FileManager.default.createDirectory(at: directory, withIntermediateDirectories: true)
    _ = try await client.downloadSnapshot(
        of: repoID,
        kind: .model,
        to: directory,
        revision: englishG2PRevision,
        matching: ["*.safetensors", "*.json"],
        progressHandler: progressHandler
    )
    // MLXAudio's cache check expects this generic file even though the G2P
    // implementation reads us_bart_config.json directly.
    try Data("{}".utf8).write(to: directory.appendingPathComponent("config.json"), options: .atomic)
    try Data(englishG2PRevision.utf8).write(to: marker, options: .atomic)
}

private struct HelperCommand: Decodable {
    let command: String
    let id: UInt64?
    let text: String?
    let rate: Float?
}

private struct HelperEvent: Encodable {
    let event: String
    let id: UInt64?
    let detail: String?
    let tokens: Int?
    let elapsedMs: UInt64?
}

private final class EventWriter: @unchecked Sendable {
    private let encoder = JSONEncoder()
    private let lock = NSLock()

    func send(
        _ event: String,
        id: UInt64? = nil,
        detail: String? = nil,
        tokens: Int? = nil,
        elapsedMs: UInt64? = nil
    ) {
        let payload = HelperEvent(
            event: event,
            id: id,
            detail: detail,
            tokens: tokens,
            elapsedMs: elapsedMs
        )
        guard let encoded = try? encoder.encode(payload) else { return }
        lock.lock()
        defer { lock.unlock() }
        FileHandle.standardOutput.write(encoded)
        FileHandle.standardOutput.write(Data([0x0A]))
    }
}

private final class ModelDownloadReporter: @unchecked Sendable {
    private let output: EventWriter
    private let lock = NSLock()
    private var lastPercent = -1

    init(output: EventWriter) {
        self.output = output
    }

    func update(_ progress: Progress) {
        let total = max(1, progress.totalUnitCount)
        let completed = max(0, progress.completedUnitCount)
        let percent = min(100, Int((completed * 100) / total))
        lock.lock()
        guard percent != lastPercent else {
            lock.unlock()
            return
        }
        lastPercent = percent
        lock.unlock()
        output.send(
            "loading",
            detail: String(
                format: "Kokoro download %d%% • %.1f / %.1f MB",
                percent,
                Double(completed) / 1_000_000,
                Double(total) / 1_000_000
            )
        )
    }
}

private enum HelperFailure: LocalizedError {
    case missingAudio
    case invalidAudioFormat

    var errorDescription: String? {
        switch self {
        case .missingAudio:
            "Kokoro completed without returning audio"
        case .invalidAudioFormat:
            "Could not create the neural playback buffer"
        }
    }
}

@MainActor
private final class NeuralAudioPlayer {
    private var engine: AVAudioEngine?
    private var player: AVAudioPlayerNode?
    private var timePitch: AVAudioUnitTimePitch?
    private var activeID: UInt64?
    private var scheduledBuffers = 0
    private var streamFinished = false

    private(set) var isPlaying = false
    private(set) var isPaused = false
    var onFinish: ((UInt64) -> Void)?

    func startStream(sampleRate: Int, roadieRate: Float, id: UInt64) throws {
        stop()

        let engine = AVAudioEngine()
        let player = AVAudioPlayerNode()
        let timePitch = AVAudioUnitTimePitch()
        guard let format = AVAudioFormat(
            commonFormat: .pcmFormatFloat32,
            sampleRate: Double(sampleRate),
            channels: 1,
            interleaved: false
        ) else {
            throw HelperFailure.invalidAudioFormat
        }

        engine.attach(player)
        engine.attach(timePitch)
        engine.connect(player, to: timePitch, format: format)
        engine.connect(timePitch, to: engine.mainMixerNode, format: format)
        timePitch.rate = min(max(roadieRate / 0.52, 0.65), 1.55)
        engine.prepare()
        try engine.start()

        self.engine = engine
        self.player = player
        self.timePitch = timePitch
        activeID = id
        isPlaying = true
        isPaused = false
        scheduledBuffers = 0
        streamFinished = false
    }

    func append(samples: [Float], sampleRate: Int, id: UInt64) throws {
        guard activeID == id, let player,
              let format = AVAudioFormat(
                  commonFormat: .pcmFormatFloat32,
                  sampleRate: Double(sampleRate),
                  channels: 1,
                  interleaved: false
              ), let buffer = AVAudioPCMBuffer(
                  pcmFormat: format,
                  frameCapacity: AVAudioFrameCount(samples.count)
              ), let channel = buffer.floatChannelData?[0] else {
            throw HelperFailure.invalidAudioFormat
        }

        buffer.frameLength = AVAudioFrameCount(samples.count)
        samples.withUnsafeBufferPointer { source in
            guard let baseAddress = source.baseAddress else { return }
            channel.update(from: baseAddress, count: samples.count)
        }

        scheduledBuffers += 1
        player.scheduleBuffer(
            buffer,
            completionCallbackType: .dataPlayedBack
        ) { [weak self] _ in
            Task { @MainActor [weak self] in
                guard let self, self.activeID == id else { return }
                self.scheduledBuffers = max(0, self.scheduledBuffers - 1)
                self.completeIfFinished(id: id)
            }
        }
        if !isPaused, !player.isPlaying {
            player.play()
        }
    }

    func finishStream(id: UInt64) {
        guard activeID == id else { return }
        streamFinished = true
        completeIfFinished(id: id)
    }

    func pause() {
        guard isPlaying, !isPaused else { return }
        player?.pause()
        isPaused = true
    }

    func resume() {
        guard isPlaying, isPaused else { return }
        player?.play()
        isPaused = false
    }

    @discardableResult
    func stop() -> UInt64? {
        let stoppedID = activeID
        activeID = nil
        isPlaying = false
        isPaused = false
        player?.stop()
        engine?.stop()
        player = nil
        timePitch = nil
        engine = nil
        scheduledBuffers = 0
        streamFinished = false
        return stoppedID
    }

    private func completeIfFinished(id: UInt64) {
        guard activeID == id, streamFinished, scheduledBuffers == 0 else { return }
        activeID = nil
        isPlaying = false
        isPaused = false
        player?.stop()
        engine?.stop()
        player = nil
        timePitch = nil
        engine = nil
        streamFinished = false
        onFinish?(id)
    }

}

@MainActor
private final class KokoroController {
    private let output: EventWriter
    private let player = NeuralAudioPlayer()
    private var model: KokoroModel?
    private var modelTask: Task<Void, Never>?
    private var generationTask: Task<Void, Never>?
    private var activeID: UInt64?

    init(output: EventWriter) {
        self.output = output
        player.onFinish = { [weak self] id in
            guard let self, self.activeID == id else { return }
            self.activeID = nil
            self.output.send("finished", id: id)
        }
    }

    func start() {
        output.send(
            "loading",
            detail: "Downloading or loading the fixed Kokoro Sarah voice for Apple Silicon"
        )
        let output = output
        modelTask = Task.detached { [weak self] in
            do {
                guard let repoID = Repo.ID(rawValue: modelIdentifier) else {
                    throw URLError(.badURL)
                }
                let cache = HubCache.default
                let client = HubClient(cache: cache)
                let progress = ModelDownloadReporter(output: output)
                try await preparePinnedEnglishG2P(
                    client: client,
                    cache: cache,
                    progressHandler: { progress.update($0) }
                )
                let modelDirectory = try await client.downloadSnapshot(
                    of: repoID,
                    kind: .model,
                    revision: modelRevision,
                    matching: ["*.safetensors", "*.json", "*.txt"],
                    progressHandler: { progress.update($0) }
                )
                let processor = KokoroMultilingualProcessor()
                let model = try await KokoroModel.fromModelDirectory(
                    modelDirectory,
                    textProcessor: processor
                )
                output.send("warming", detail: "Prewarming Kokoro MLX kernels")
                _ = try await model.generate(
                    text: "Roadie is ready for A W S, Supa base, S S H, and A P I work.",
                    voice: modelVoice,
                    refAudio: nil,
                    refText: nil,
                    language: "en-us",
                    generationParameters: model.defaultGenerationParameters
                )
                try Task.checkCancellation()
                await self?.didLoad(model)
            } catch is CancellationError {
                return
            } catch {
                output.send(
                    "error",
                    detail: "Could not load Kokoro: \(error.localizedDescription)"
                )
            }
        }
    }

    func handle(_ command: HelperCommand) {
        switch command.command {
        case "speak":
            guard let id = command.id,
                  let text = command.text?.trimmingCharacters(in: .whitespacesAndNewlines),
                  !text.isEmpty else {
                output.send("error", detail: "Kokoro received an empty speech request")
                return
            }
            speak(id: id, text: text, rate: command.rate ?? 0.52)
        case "pause":
            pause()
        case "continue":
            resume()
        case "stop":
            stopActive(emitCancellation: true)
        case "shutdown":
            break
        default:
            output.send("error", detail: "Unknown TTS helper command: \(command.command)")
        }
    }

    func shutdown() {
        modelTask?.cancel()
        modelTask = nil
        stopActive(emitCancellation: false)
        model = nil
        output.send("stopping", detail: "Kokoro helper stopped")
    }

    private func didLoad(_ model: KokoroModel) {
        modelTask = nil
        self.model = model
        output.send(
            "ready",
            detail: "Kokoro Sarah ready • fixed female voice • MLX GPU • \(model.sampleRate / 1_000) kHz"
        )
    }

    private func speak(id: UInt64, text: String, rate: Float) {
        guard let model else {
            output.send(
                "error",
                id: id,
                detail: "Kokoro is still loading; use the Apple fallback"
            )
            return
        }

        stopActive(emitCancellation: true)
        activeID = id
        output.send("generating", id: id, detail: "Synthesizing locally", tokens: 0)
        let output = output
        generationTask = Task.detached { [weak self] in
            let startedAt = Date()
            var receivedAudio = false
            var generatedTokens = 0
            do {
                let stream = model.generateStream(
                    text: text,
                    voice: modelVoice,
                    refAudio: nil,
                    refText: nil,
                    language: "en-us",
                    generationParameters: model.defaultGenerationParameters
                )
                for try await event in stream {
                    try Task.checkCancellation()
                    switch event {
                    case .token:
                        generatedTokens += 1
                        if generatedTokens == 1 || generatedTokens.isMultiple(of: 8) {
                            let milliseconds = UInt64(
                                max(0, Date().timeIntervalSince(startedAt) * 1_000)
                            )
                            output.send(
                                "generating",
                                id: id,
                                detail: "Synthesizing locally",
                                tokens: generatedTokens,
                                elapsedMs: milliseconds
                            )
                        }
                    case .info(let info):
                        output.send(
                            "generated",
                            id: id,
                            detail: String(format: "Generated %.1f tokens/s", info.tokensPerSecond),
                            tokens: info.generationTokenCount,
                            elapsedMs: UInt64(max(0, info.generateTime * 1_000))
                        )
                    case .audio(let audio):
                        let samples = audio.asArray(Float.self)
                        guard !samples.isEmpty else { continue }
                        try await self?.playChunk(
                            id: id,
                            samples: samples,
                            sampleRate: model.sampleRate,
                            rate: rate
                        )
                        receivedAudio = true
                    }
                }
                try Task.checkCancellation()
                guard receivedAudio else {
                    throw HelperFailure.missingAudio
                }
                await self?.finishGeneration(id: id)
            } catch is CancellationError {
                await self?.didCancelGeneration(id: id)
            } catch {
                await self?.didFailGeneration(id: id, error: error)
            }
        }
    }

    private func playChunk(
        id: UInt64,
        samples: [Float],
        sampleRate: Int,
        rate: Float
    ) throws {
        guard activeID == id else { return }
        if !player.isPlaying {
            try player.startStream(sampleRate: sampleRate, roadieRate: rate, id: id)
            output.send("started", id: id, detail: "Kokoro Sarah fixed-voice playback")
        }
        try player.append(samples: samples, sampleRate: sampleRate, id: id)
    }

    private func finishGeneration(id: UInt64) {
        guard activeID == id else { return }
        generationTask = nil
        player.finishStream(id: id)
    }

    private func pause() {
        guard let id = activeID, player.isPlaying, !player.isPaused else { return }
        player.pause()
        output.send("paused", id: id)
    }

    private func resume() {
        guard let id = activeID, player.isPlaying, player.isPaused else { return }
        player.resume()
        output.send("resumed", id: id)
    }

    private func stopActive(emitCancellation: Bool) {
        guard let id = activeID else { return }
        activeID = nil
        generationTask?.cancel()
        generationTask = nil
        _ = player.stop()
        if emitCancellation {
            output.send("cancelled", id: id)
        }
    }

    private func didCancelGeneration(id: UInt64) {
        guard activeID == id else { return }
        activeID = nil
        generationTask = nil
        output.send("cancelled", id: id)
    }

    private func didFailGeneration(id: UInt64, error: Error) {
        guard activeID == id else { return }
        activeID = nil
        generationTask = nil
        _ = player.stop()
        output.send(
            "error",
            id: id,
            detail: "Kokoro generation failed: \(error.localizedDescription)"
        )
    }
}

@main
private struct RoadieTTS {
    static func main() async {
        let output = EventWriter()
        let controller = await MainActor.run { KokoroController(output: output) }
        await MainActor.run { controller.start() }

        let input = Task.detached {
            let decoder = JSONDecoder()
            while let line = readLine() {
                guard let data = line.data(using: .utf8) else { continue }
                do {
                    let command = try decoder.decode(HelperCommand.self, from: data)
                    await MainActor.run { controller.handle(command) }
                    if command.command == "shutdown" {
                        break
                    }
                } catch {
                    output.send("error", detail: "Invalid TTS helper command: \(error.localizedDescription)")
                }
            }
        }
        await input.value
        await MainActor.run { controller.shutdown() }
    }
}
