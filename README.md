# Интеграционная шина

Интеграционный слой между системами компании. На этапе MVP платформа работает в Docker на одном Linux-сервере. SAP Business One и остальные учётные системы остаются на своих местах.

Предварительный план: [docs/preliminary-plan.md](docs/preliminary-plan.md).

Правила общения в проекте: [.cursor/rules/communication.mdc](.cursor/rules/communication.mdc). Термины поясняются при первом упоминании, у каждого решения есть краткая причина.

Текущее решение для старта:

- брокер маршрутов — Redpanda (Kafka API, один узел);
- Apache Kafka на хосте уже стоит, в обмен этапа 1 не входит;
- адаптеры SAP Business One и 1С — Apache Camel;
- этап 1 идёт на эмуляторах SAP Business One, 1С, EDIN и Вчасно.
