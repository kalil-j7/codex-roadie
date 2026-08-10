// swift-tools-version: 6.2
import PackageDescription

let package = Package(
    name: "RoadieTTS",
    platforms: [.macOS(.v14)],
    dependencies: [
        .package(
            url: "https://github.com/Blaizzy/mlx-audio-swift.git",
            exact: "0.1.3"
        ),
        .package(
            url: "https://github.com/ml-explore/mlx-swift.git",
            .upToNextMajor(from: "0.30.6")
        ),
        .package(
            url: "https://github.com/huggingface/swift-huggingface.git",
            exact: "0.9.0"
        ),
    ],
    targets: [
        .executableTarget(
            name: "roadie-tts",
            dependencies: [
                .product(name: "MLXAudioCore", package: "mlx-audio-swift"),
                .product(name: "MLXAudioTTS", package: "mlx-audio-swift"),
                .product(name: "MLX", package: "mlx-swift"),
                .product(name: "HuggingFace", package: "swift-huggingface"),
            ]
        ),
    ],
    swiftLanguageModes: [.v5]
)
