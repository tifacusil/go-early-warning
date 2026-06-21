-- early_warning schema (database-per-service, CLAUDE.md §1.2).
-- Migracion versionada con golang-migrate (CLAUDE.md §2.4) — reemplaza el
-- CREATE TABLE IF NOT EXISTS en runtime del baseline Python.

CREATE TABLE IF NOT EXISTS alerts (
    id BIGINT PRIMARY KEY AUTO_INCREMENT,
    created_at DATETIME NOT NULL,
    message_id VARCHAR(64),
    project_id VARCHAR(64) NOT NULL,
    sensor_id VARCHAR(64) NOT NULL,
    alert_level ENUM('blue','orange','red') NOT NULL,
    deformation_speed_mm_day DOUBLE,
    threshold_mm_day DOUBLE,
    alert_timestamp DATETIME,
    UNIQUE KEY uk_alert_message (message_id),
    INDEX idx_alerts_project_ts (project_id, alert_timestamp)
);

CREATE TABLE IF NOT EXISTS reports (
    id BIGINT PRIMARY KEY AUTO_INCREMENT,
    report_id VARCHAR(64) NOT NULL,
    project_id VARCHAR(64) NOT NULL,
    period ENUM('daily','weekly') NOT NULL,
    window_start DATETIME NOT NULL,
    window_end DATETIME NOT NULL,
    generated_at DATETIME NOT NULL,
    content_json JSON NOT NULL,
    UNIQUE KEY uk_report (project_id, period, window_start, window_end),
    INDEX idx_reports_project_period (project_id, period, generated_at)
);
