use crate::remote::WorkerEngine;

#[derive(Debug, Clone, PartialEq, Eq)]
pub enum VoiceIntent {
    NotAddressed,
    WakeOnly,
    Prompt(String),
    Cancel,
    StopSpeaking,
    NewSession,
    Repeat,
    Status,
    SetMacWorkdir(String),
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct RoutedVoiceIntent {
    pub intent: VoiceIntent,
    pub engine: Option<WorkerEngine>,
}

pub fn parse_voice_intent(transcript: &str, wake_phrase: &str, armed: bool) -> VoiceIntent {
    parse_routed_voice_intent(transcript, wake_phrase, armed).intent
}

pub fn parse_routed_voice_intent(
    transcript: &str,
    wake_phrase: &str,
    armed: bool,
) -> RoutedVoiceIntent {
    let normalized = normalize(transcript);
    if normalized.is_empty() {
        return routed(VoiceIntent::NotAddressed, None);
    }

    let wake_aliases = wake_aliases(wake_phrase);
    let lowercased = transcript.to_lowercase();
    let addressed = wake_aliases
        .iter()
        .filter_map(|(wake, engine)| {
            lowercased.find(wake).map(|index| {
                (
                    index,
                    wake.len(),
                    trim_command(&transcript[index + wake.len()..]).to_owned(),
                    *engine,
                )
            })
        })
        .min_by_key(|(index, length, _, _)| (*index, std::cmp::Reverse(*length)))
        .map(|(_, _, command, engine)| (command, engine))
        .or_else(|| {
            fuzzy_worker_command(transcript).map(|(command, engine)| (command, Some(engine)))
        })
        .or_else(|| armed.then(|| (trim_command(transcript).to_owned(), None)));

    let Some((command, engine)) = addressed else {
        return routed(VoiceIntent::NotAddressed, None);
    };
    if command.is_empty() {
        return routed(VoiceIntent::WakeOnly, engine);
    }

    let normalized_command = normalize(&command);
    if is_cancel_command(&normalized_command) {
        return routed(VoiceIntent::Cancel, engine);
    }
    if let Some(workspace) = mac_workdir_command(&command) {
        return routed(VoiceIntent::SetMacWorkdir(workspace.to_owned()), engine);
    }

    let intent = match normalized_command.as_str() {
        "stop speaking" | "be quiet" | "quiet" => VoiceIntent::StopSpeaking,
        "new session" | "start a new session" | "fresh session" => VoiceIntent::NewSession,
        "repeat" | "repeat that" | "say that again" => VoiceIntent::Repeat,
        "status" | "what is your status" | "are you working" => VoiceIntent::Status,
        _ => VoiceIntent::Prompt(command),
    };
    routed(intent, engine)
}

fn mac_workdir_command(command: &str) -> Option<&str> {
    let lowercased = command.to_ascii_lowercase();
    let command = command.trim();
    let lowercased = lowercased.trim();
    let polite_prefix = ["can you ", "could you ", "please "]
        .into_iter()
        .find(|prefix| lowercased.starts_with(prefix));
    let offset = polite_prefix.map_or(0, str::len);
    let lowercased = &lowercased[offset..];
    let command = &command[offset..];
    for prefix in [
        "set mac workdir to ",
        "set the mac workdir to ",
        "set mac working directory to ",
        "set the mac working directory to ",
        "set workdir to ",
        "set the workdir to ",
        "set working directory to ",
        "set the working directory to ",
        "change mac workdir to ",
        "change the mac workdir to ",
        "change working directory to ",
        "change the working directory to ",
    ] {
        if lowercased.starts_with(prefix) {
            let workspace = trim_command(&command[prefix.len()..]);
            return (!workspace.is_empty()).then_some(workspace);
        }
    }
    None
}

const fn routed(intent: VoiceIntent, engine: Option<WorkerEngine>) -> RoutedVoiceIntent {
    RoutedVoiceIntent { intent, engine }
}

fn is_cancel_command(command: &str) -> bool {
    matches!(
        command,
        "cancel"
            | "cancel task"
            | "cancel the task"
            | "cancel current task"
            | "cancel the current task"
            | "cancel it"
            | "cancel this"
            | "cancel that"
            | "cancelled"
            | "canceled"
            | "please cancel"
            | "can you cancel"
            | "could you cancel"
            | "stop"
            | "stop task"
            | "stop the task"
            | "stop current task"
            | "stop the current task"
            | "stop working"
    )
}

fn trim_command(value: &str) -> &str {
    value.trim_matches(|character: char| {
        character.is_whitespace()
            || matches!(
                character,
                ',' | '.' | ':' | ';' | '!' | '?' | '-' | '–' | '—'
            )
    })
}

fn wake_aliases(wake_phrase: &str) -> Vec<(String, Option<WorkerEngine>)> {
    let configured = normalize(wake_phrase);
    let configured_engine = wake_engine(&configured);
    let mut aliases = vec![(configured, configured_engine)];
    for (alias, engine) in [
        ("hey codex", WorkerEngine::Codex),
        ("hey code x", WorkerEngine::Codex),
        ("hey codecs", WorkerEngine::Codex),
        ("hey kodex", WorkerEngine::Codex),
        ("hey claude", WorkerEngine::Claude),
        ("hey cloud", WorkerEngine::Claude),
        ("hey claud", WorkerEngine::Claude),
        ("hey clawed", WorkerEngine::Claude),
    ] {
        if !aliases.iter().any(|(existing, _)| existing == alias) {
            aliases.push((alias.to_owned(), Some(engine)));
        }
    }
    aliases.sort_by_key(|(alias, _)| std::cmp::Reverse(alias.len()));
    aliases
}

fn wake_engine(wake_phrase: &str) -> Option<WorkerEngine> {
    match wake_phrase {
        "hey codex" | "hey code x" | "hey codecs" | "hey kodex" => Some(WorkerEngine::Codex),
        "hey claude" | "hey cloud" | "hey claud" | "hey clawed" => Some(WorkerEngine::Claude),
        _ => None,
    }
}

fn normalize(value: &str) -> String {
    value
        .chars()
        .map(|character| {
            if character.is_alphanumeric() || character.is_whitespace() {
                character.to_ascii_lowercase()
            } else {
                ' '
            }
        })
        .collect::<String>()
        .split_whitespace()
        .collect::<Vec<_>>()
        .join(" ")
}

fn fuzzy_worker_command(transcript: &str) -> Option<(String, WorkerEngine)> {
    let words = word_spans(transcript);
    for (index, (_, _, word)) in words.iter().enumerate() {
        if !matches!(word.as_str(), "hey" | "hay") {
            continue;
        }
        let Some((_, next_end, next_word)) = words.get(index + 1) else {
            continue;
        };
        let mut wake_end = *next_end;
        let mut candidate = next_word.clone();
        if candidate == "code"
            && let Some((_, third_end, third_word)) = words.get(index + 2)
            && third_word == "x"
        {
            candidate.push('x');
            wake_end = *third_end;
        }
        let phonetic_candidate = canonicalize_codex_candidate(&candidate);
        if (4..=7).contains(&candidate.len()) && edit_distance(&phonetic_candidate, "codex") <= 2 {
            return Some((
                trim_command(&transcript[wake_end..]).to_owned(),
                WorkerEngine::Codex,
            ));
        }
        let claude_candidate = canonicalize_claude_candidate(&candidate);
        if (4..=7).contains(&candidate.len()) && edit_distance(&claude_candidate, "claude") <= 1 {
            return Some((
                trim_command(&transcript[wake_end..]).to_owned(),
                WorkerEngine::Claude,
            ));
        }
    }
    None
}

fn canonicalize_codex_candidate(value: &str) -> String {
    let mut candidate = value.to_owned();
    if candidate.starts_with('k') {
        candidate.replace_range(..1, "c");
    }
    for suffix in ["cks", "ks", "cs"] {
        if candidate.ends_with(suffix) {
            candidate.truncate(candidate.len() - suffix.len());
            candidate.push('x');
            break;
        }
    }
    candidate
}

fn canonicalize_claude_candidate(value: &str) -> String {
    match value {
        "cloud" | "clawed" | "clod" => "claude".to_owned(),
        _ => value.to_owned(),
    }
}

fn word_spans(value: &str) -> Vec<(usize, usize, String)> {
    let mut words = Vec::new();
    let mut start = None;
    for (index, character) in value
        .char_indices()
        .chain(std::iter::once((value.len(), ' ')))
    {
        if character.is_alphanumeric() {
            start.get_or_insert(index);
        } else if let Some(word_start) = start.take() {
            words.push((word_start, index, value[word_start..index].to_lowercase()));
        }
    }
    words
}

fn edit_distance(left: &str, right: &str) -> usize {
    let mut previous = (0..=right.chars().count()).collect::<Vec<_>>();
    for (left_index, left_character) in left.chars().enumerate() {
        let mut current = vec![left_index + 1];
        for (right_index, right_character) in right.chars().enumerate() {
            let insertion = current[right_index] + 1;
            let deletion = previous[right_index + 1] + 1;
            let substitution =
                previous[right_index] + usize::from(left_character != right_character);
            current.push(insertion.min(deletion).min(substitution));
        }
        previous = current;
    }
    previous.last().copied().unwrap_or_default()
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn extracts_prompt_after_wake_phrase() {
        assert_eq!(
            parse_voice_intent("Hey Codex, run the mobile tests.", "hey codex", false),
            VoiceIntent::Prompt("run the mobile tests".into())
        );
    }

    #[test]
    fn cancel_control_phrase_is_recognized_with_common_whisper_variants() {
        for transcript in [
            "Hey Codex cancel",
            "Hey Codex, cancel it.",
            "Hey Kodex, cancel the current task",
            "Hey code X, can you cancel?",
            "Hey Codex, stop working",
        ] {
            assert_eq!(
                parse_voice_intent(transcript, "hey codex", false),
                VoiceIntent::Cancel,
                "{transcript}"
            );
        }
    }

    #[test]
    fn accepts_common_codex_misrecognition() {
        assert_eq!(
            parse_voice_intent("Hey code X new session", "hey codex", false),
            VoiceIntent::NewSession
        );
        assert_eq!(
            parse_voice_intent("Hey Kodex, status", "hey codex", false),
            VoiceIntent::Status
        );
        assert_eq!(
            parse_voice_intent("Hay, Kodeks — run the tests", "hey codex", false),
            VoiceIntent::Prompt("run the tests".into())
        );
        assert_eq!(
            parse_voice_intent("Hey, code X: repeat that", "hey codex", false),
            VoiceIntent::Repeat
        );
    }

    #[test]
    fn fuzzy_wake_matching_still_rejects_unrelated_speech() {
        assert_eq!(
            parse_voice_intent("Hey there, run the tests", "hey codex", false),
            VoiceIntent::NotAddressed
        );
    }

    #[test]
    fn ignores_ambient_conversation_without_wake_phrase() {
        assert_eq!(
            parse_voice_intent("run the tests", "hey codex", false),
            VoiceIntent::NotAddressed
        );
    }

    #[test]
    fn armed_follow_up_does_not_need_wake_phrase() {
        assert_eq!(
            parse_voice_intent("show me the current git status", "hey codex", true),
            VoiceIntent::Prompt("show me the current git status".into())
        );
    }

    #[test]
    fn preserves_prompt_case_and_path_punctuation() {
        assert_eq!(
            parse_voice_intent("Hey Codex, inspect README.md in JoyApp", "hey codex", false),
            VoiceIntent::Prompt("inspect README.md in JoyApp".into())
        );
    }

    #[test]
    fn extracts_persistent_mac_workdir_control_with_literal_path() {
        assert_eq!(
            parse_voice_intent(
                "Hey Codex, set Mac workdir to /Users/example/sample-project",
                "hey codex",
                false,
            ),
            VoiceIntent::SetMacWorkdir("/Users/example/sample-project".into())
        );
    }

    #[test]
    fn extracts_polite_mac_workdir_control_with_spoken_path() {
        assert_eq!(
            parse_voice_intent(
                "Hey Codex, can you set the working directory to slash Users slash example slash sample dash project",
                "hey codex",
                false,
            ),
            VoiceIntent::SetMacWorkdir(
                "slash Users slash example slash sample dash project".into()
            )
        );
    }

    #[test]
    fn routes_each_named_wake_phrase_to_its_worker() {
        assert_eq!(
            parse_routed_voice_intent("Hey Codex, run the tests", "hey codex", false),
            RoutedVoiceIntent {
                intent: VoiceIntent::Prompt("run the tests".into()),
                engine: Some(WorkerEngine::Codex),
            }
        );
        assert_eq!(
            parse_routed_voice_intent("Hey Claude, inspect the app", "hey codex", false),
            RoutedVoiceIntent {
                intent: VoiceIntent::Prompt("inspect the app".into()),
                engine: Some(WorkerEngine::Claude),
            }
        );
    }

    #[test]
    fn accepts_common_claude_transcriptions() {
        for transcript in [
            "Hey Cloud, status",
            "Hey Claud, status",
            "Hey Clawed, status",
        ] {
            assert_eq!(
                parse_routed_voice_intent(transcript, "hey codex", false),
                RoutedVoiceIntent {
                    intent: VoiceIntent::Status,
                    engine: Some(WorkerEngine::Claude),
                },
                "{transcript}"
            );
        }
    }

    #[test]
    fn custom_wake_phrase_uses_the_configured_default_worker() {
        assert_eq!(
            parse_routed_voice_intent("Roadie, run QA", "roadie", false),
            RoutedVoiceIntent {
                intent: VoiceIntent::Prompt("run QA".into()),
                engine: None,
            }
        );
    }
}
