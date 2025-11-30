//! Terminal UI Dashboard for POS Cluster Nodes
//!
//! Provides real-time visualization of:
//! - Node throughput and shard assignment
//! - Transaction routing (accepted vs forwarded)
//! - Bridge traffic between shards
//! - Live event logs

use crossterm::{
    event::{self, Event, KeyCode},
    execute,
    terminal::{disable_raw_mode, enable_raw_mode, EnterAlternateScreen, LeaveAlternateScreen},
};
use ratatui::{
    backend::CrosstermBackend,
    layout::{Constraint, Direction, Layout},
    style::{Color, Modifier, Style},
    text::{Line, Span},
    widgets::{Block, Borders, List, ListItem, Paragraph},
    Frame, Terminal,
};
use std::io;
use std::sync::{Arc, Mutex};
use std::time::Duration;

/// Shared state for the UI
#[derive(Clone)]
pub struct AppState {
    pub node_id: String,
    pub shard_id: u8,          // 0 or 1
    pub tps: u64,
    pub accepted_count: u64,   // Processed locally
    pub forwarded_count: u64,  // Bridged to peer
    pub received_count: u64,   // Received from bridge
    pub mempool_size: usize,
    pub peer_count: usize,
    pub logs: Vec<String>,
}

impl AppState {
    pub fn new(id: &str, shard: u8) -> Self {
        Self {
            node_id: id.to_string(),
            shard_id: shard,
            tps: 0,
            accepted_count: 0,
            forwarded_count: 0,
            received_count: 0,
            mempool_size: 0,
            peer_count: 0,
            logs: Vec::new(),
        }
    }

    pub fn log(&mut self, msg: String) {
        if self.logs.len() > 20 {
            self.logs.remove(0);
        }
        self.logs.push(msg);
    }
}

/// Run the TUI event loop
pub fn run_tui(state: Arc<Mutex<AppState>>) -> io::Result<()> {
    // Setup terminal
    enable_raw_mode()?;
    let mut stdout = io::stdout();
    execute!(stdout, EnterAlternateScreen)?;
    let backend = CrosstermBackend::new(stdout);
    let mut terminal = Terminal::new(backend)?;

    // Event loop
    loop {
        terminal.draw(|f| ui(f, &state.lock().unwrap()))?;

        if event::poll(Duration::from_millis(100))? {
            if let Event::Key(key) = event::read()? {
                if key.code == KeyCode::Char('q') {
                    break;
                }
            }
        }
    }

    // Restore terminal
    disable_raw_mode()?;
    execute!(terminal.backend_mut(), LeaveAlternateScreen)?;
    terminal.show_cursor()?;

    Ok(())
}

/// Render the UI
fn ui(f: &mut Frame, app: &AppState) {
    let chunks = Layout::default()
        .direction(Direction::Vertical)
        .constraints([
            Constraint::Length(3),  // Header
            Constraint::Min(0),      // Metrics
            Constraint::Length(12),  // Logs
        ])
        .split(f.area());

    // === HEADER ===
    let color = if app.shard_id == 0 {
        Color::Cyan
    } else {
        Color::Magenta
    };

    let title = Paragraph::new(format!(
        " ⚡ POS CLUSTER: {} | SHARD {} ",
        app.node_id, app.shard_id
    ))
    .block(
        Block::default()
            .borders(Borders::ALL)
            .style(Style::default().fg(color)),
    );
    f.render_widget(title, chunks[0]);

    // === METRICS ===
    let grid = Layout::default()
        .direction(Direction::Horizontal)
        .constraints([Constraint::Percentage(50), Constraint::Percentage(50)])
        .split(chunks[1]);

    // Left: Throughput
    let tps_block = Block::default()
        .title(" Throughput ")
        .borders(Borders::ALL);

    let stats = vec![
        Line::from(""),
        Line::from(vec![
            Span::raw("  🚀 TPS:            "),
            Span::styled(
                format!("{}", app.tps),
                Style::default()
                    .fg(Color::Green)
                    .add_modifier(Modifier::BOLD),
            ),
        ]),
        Line::from(vec![
            Span::raw("  📦 Total Valid:    "),
            Span::styled(
                format!("{}", app.accepted_count),
                Style::default().fg(Color::Yellow),
            ),
        ]),
        Line::from(vec![
            Span::raw("  📥 Bridged In:     "),
            Span::styled(
                format!("{}", app.received_count),
                Style::default().fg(Color::Blue),
            ),
        ]),
    ];
    f.render_widget(Paragraph::new(stats).block(tps_block), grid[0]);

    // Right: Routing
    let bridge_block = Block::default()
        .title(" Routing ")
        .borders(Borders::ALL);

    let range_start = if app.shard_id == 0 { "0%" } else { "50%" };
    let range_end = if app.shard_id == 0 { "50%" } else { "100%" };

    let routing = vec![
        Line::from(""),
        Line::from(vec![
            Span::raw("  🔄 Bridged Out:    "),
            Span::styled(
                format!("{}", app.forwarded_count),
                Style::default().fg(Color::Red),
            ),
        ]),
        Line::from(vec![
            Span::raw("  🔗 Peers:          "),
            Span::styled(
                format!("{}", app.peer_count),
                Style::default().fg(Color::Cyan),
            ),
        ]),
        Line::from(vec![
            Span::raw("  🛡️  My Sector:      "),
            Span::styled(
                format!("{} - {}", range_start, range_end),
                Style::default()
                    .fg(color)
                    .add_modifier(Modifier::BOLD),
            ),
        ]),
        Line::from(vec![
            Span::raw("  💾 Mempool:        "),
            Span::styled(
                format!("{}", app.mempool_size),
                Style::default().fg(Color::White),
            ),
        ]),
    ];
    f.render_widget(Paragraph::new(routing).block(bridge_block), grid[1]);

    // === LOGS ===
    let logs: Vec<ListItem> = app
        .logs
        .iter()
        .rev()
        .map(|m| ListItem::new(format!("> {}", m)))
        .collect();

    let log_list = List::new(logs).block(Block::default().title(" Logs ").borders(Borders::ALL));
    f.render_widget(log_list, chunks[2]);
}
