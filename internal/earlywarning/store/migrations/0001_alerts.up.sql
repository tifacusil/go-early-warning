-- early_warning schema — tabla de alertas (database-per-service, CLAUDE.md §1.2/§2.4).
-- Un statement por archivo: el driver mysql de golang-migrate ejecuta el archivo
-- como una sola sentencia; varias separadas por ';' darian error 1064.
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
)
