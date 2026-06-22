-- early_warning schema — tabla de reportes (un statement por archivo).
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
)
