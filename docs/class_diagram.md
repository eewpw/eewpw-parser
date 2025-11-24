# Class Diagrams

```mermaid
classDiagram
    class FinderParser {
      -cfg: Dict
      -dialect: str
      +parse(inputs, sink=None) FinalDoc|None
    }
    class FinderStreamState {
      buffer: List~str~
      pending_station_list: List~tuple~
      version_by_event: Dict
      file_start_ts_iso: str
      file_end_ts_iso: str
    }
    class FinderBaseDialect {
      +parse_file(path) (List~Detection~, List~Annotation~, Dict)
      +parse_stream(lines, state, finalize)
      -_parse_annotations(lines)
      -_parse_detections(lines)
      -_pick_detection_timestamp(block_lines, emission_ts, orig_time)
    }
    class SCFinderDialect
    class ShakeAlertFinderDialect
    class NativeFinderDialect
    class NativeFinderLegacyDialect
    class Detection
    class Annotation
    FinderParser --> FinderBaseDialect : uses
    FinderBaseDialect --> FinderStreamState
    FinderBaseDialect <|-- SCFinderDialect
    FinderBaseDialect <|-- ShakeAlertFinderDialect
    FinderBaseDialect <|-- NativeFinderDialect
    FinderBaseDialect <|-- NativeFinderLegacyDialect
    FinderBaseDialect --> Detection
    FinderBaseDialect --> Annotation
    FinderParser --> BaseSink
```

```mermaid
classDiagram
    class VSParser {
      -cfg: Dict
      -dialect: str
      +parse(inputs, sink=None) FinalDoc|None
    }
    class VSDialect {
      +parse_file(path) (List~Detection~, List~Annotation~, Dict)
      +feed_line(line, state)
      +flush(state)
    }
    class VSStreamState {
      current_event: VSEventState
      version_by_event: Dict
      file_start_ts_iso: str
      file_end_ts_iso: str
    }
    class VSEventState {
      event_id: str
      update_number: int
      last_ts_iso: str
      +start_station(...)
      +flush_station()
      +to_detection(version_by_event) Detection
    }
    class Detection
    class Annotation
    VSParser --> VSDialect : uses
    VSDialect --> VSStreamState
    VSStreamState --> VSEventState
    VSDialect --> Detection
    VSDialect --> Annotation
    VSParser --> BaseSink
```

```mermaid
classDiagram
    class BaseSink {
      +start_run()
      +emit_detection(det)
      +emit_annotation(profile, ann)
      +finalize(meta)
    }
    class FinalDocSink {
      +start_run()
      +emit_detection(det)
      +emit_annotation(profile, ann)
      +finalize(meta) FinalDoc
    }
    class JsonlStreamSink {
      +start_run()
      +emit_detection(det)
      +emit_annotation(profile, ann)
      +finalize(meta)
    }
    class SleepSink {
      +start_run()
      +emit_detection(det)
      +emit_annotation(profile, ann)
      +finalize(meta)
    }
    BaseSink <|.. FinalDocSink
    BaseSink <|.. JsonlStreamSink
    BaseSink <|.. SleepSink
    SleepSink --> JsonlStreamSink : wraps
```
