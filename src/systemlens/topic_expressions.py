"""Helpers for Kafka topic expressions embedded in Spring annotations/calls."""

from dataclasses import dataclass
import re


@dataclass(frozen=True)
class SpringTopicReference:
    property_key: str
    display_name: str


_SPRING_PLACEHOLDER_RE = re.compile(r"^\$\{\s*([^}]+?)\s*\}$")
_SPRING_SPEL_RE = re.compile(r"^#\{\s*(.*?)\s*\}$")
_BARE_PROPERTY_KEY_RE = re.compile(r"^[A-Za-z_][\w.-]*$")


def _strip_matching_quotes(value: str) -> str:
    value = value.strip()
    if len(value) >= 2 and value[0] == value[-1] and value[0] in {"'", '"'}:
        return value[1:-1].strip()
    return value


def _display_name(property_expression: str) -> str:
    key, _, _default = property_expression.partition(":")
    return key.strip()


def spring_topic_reference(value: str) -> SpringTopicReference | None:
    """Return the property key represented by Spring placeholder/SpEL syntax.

    Supported forms include `${kafka.topic}`, `#{kafka.topic}` and
    `#{'${kafka.topic}'}`.  The returned display name deliberately omits a
    Spring default suffix, so `${kafka.topic:orders}` is reported as
    `kafka.topic` when it cannot be resolved.
    """
    expression = _strip_matching_quotes(value)
    spel = _SPRING_SPEL_RE.match(expression)
    if spel is not None:
        expression = _strip_matching_quotes(spel.group(1))

    placeholder = _SPRING_PLACEHOLDER_RE.match(expression)
    if placeholder is not None:
        property_key = placeholder.group(1).strip()
        return SpringTopicReference(property_key, _display_name(property_key))

    if spel is not None and _BARE_PROPERTY_KEY_RE.fullmatch(expression):
        return SpringTopicReference(expression, expression)

    return None
