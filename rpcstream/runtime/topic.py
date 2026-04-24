from dataclasses import dataclass

@dataclass
class TopicSet:
    main: str
    dlq: str


@dataclass
class TopicMaps:
    main: dict[str, str]
    dlq: dict[str, str]


def normalize_entity(entity: str) -> str:
    return entity.strip().lower()


def build_topics(cfg, entity: str) -> TopicSet:

    template = (
        cfg.kafka.common.topic_template
        or "{type}.{chain}.{network}.{kind}_{entity}"
    )

    def render(kind):
        return template.format(
            chain=cfg.chain.name,
            network=cfg.chain.network,
            type=cfg.chain.type,
            entity=entity,
            kind=kind,
        )

    return TopicSet(
        main=render("raw"),
        dlq=render("dlq"),
    )
