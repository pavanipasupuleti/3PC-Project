"""
Metrics dashboard using Flask + Plotly.

Real-time visualization of 3PC protocol metrics.
"""

from flask import Flask, render_template, jsonify
import plotly
import plotly.graph_objs as go
import json
import requests

app = Flask(__name__)


# ---------------------------------------------------------
# Dynamic leader-aware metrics fetching
# ---------------------------------------------------------
def fetch_metrics_from_leader():
    """
    Discover current leader dynamically
    and fetch metrics from active coordinator.
    """

    coordinators = [
        ("coordinator", 5000),
        ("coordinator-2", 5010),
        ("coordinator-3", 5020)
    ]

    try:
        current_leader = None

        # Ask all coordinators who leader is
        for host, port in coordinators:

            try:
                response = requests.get(
                    f'http://{host}:{port}/leader-status',
                    timeout=2
                )

                if response.status_code == 200:
                    data = response.json()

                    current_leader = data.get("current_leader")

                    if current_leader:
                        break

            except Exception:
                continue

        if not current_leader:
            return _empty_metrics()

        leader_map = {
            "coordinator-1": ("coordinator", 5000),
            "coordinator-2": ("coordinator-2", 5010),
            "coordinator-3": ("coordinator-3", 5020)
        }

        leader_host, leader_port = leader_map[current_leader]

        # Fetch metrics from actual leader
        metrics_response = requests.get(
            f'http://{leader_host}:{leader_port}/metrics',
            timeout=2
        )

        if metrics_response.status_code == 200:
            return metrics_response.json()

        return _empty_metrics()

    except Exception as e:
        print(f"Dashboard fetch error: {e}")
        return _empty_metrics()


@app.route('/')
def dashboard():
    """Main dashboard page."""
    return render_template('dashboard.html')


@app.route('/api/metrics')
def get_metrics():
    """API endpoint for metrics data."""
    snapshot = fetch_metrics_from_leader()
    return jsonify(snapshot)


def _empty_metrics():
    """Return empty metrics structure."""
    return {
        "transactions": {
            "total": 0,
            "committed": 0,
            "aborted": 0,
            "commit_rate": 0
        },
        "latency": {
            "phase1_avg": 0,
            "phase2_avg": 0,
            "phase3_avg": 0,
            "total_avg": 0,
            "phase1_data": [],
            "phase2_data": [],
            "phase3_data": []
        },
        "failures": {
            "partitions": 0,
            "timeouts": 0
        },
        "state_transitions": {},
        "timeline": []
    }


@app.route('/api/charts/commit-rate')
def commit_rate_chart():
    """Commit rate pie chart."""

    snapshot = fetch_metrics_from_leader()

    txns = snapshot['transactions']

    committed = txns['committed']
    aborted = txns['aborted']

    if committed == 0 and aborted == 0:
        pie_values = [1]
        pie_labels = ['No Data']
        pie_colors = ['#444444']
    else:
        pie_values = [committed, aborted] if aborted > 0 else [committed]
        pie_labels = ['Committed', 'Aborted'] if aborted > 0 else ['Committed']
        pie_colors = ['#00ff88', '#ff4444'] if aborted > 0 else ['#00ff88']

    fig = go.Figure(data=[go.Pie(
        labels=pie_labels,
        values=pie_values,
        marker=dict(colors=pie_colors),
        hole=0.3
    )])

    fig.update_layout(
        title='Transaction Outcomes',
        paper_bgcolor='rgba(0,0,0,0)',
        plot_bgcolor='rgba(0,0,0,0)',
        font=dict(color='white')
    )

    return json.dumps(fig, cls=plotly.utils.PlotlyJSONEncoder)


@app.route('/api/charts/latency')
def latency_chart():
    """Phase latency bar chart."""

    snapshot = fetch_metrics_from_leader()

    lat = snapshot['latency']

    fig = go.Figure(data=[
        go.Bar(
            name='Phase 1 (Voting)',
            x=['Phase 1'],
            y=[lat['phase1_avg']],
            marker_color='#3498db'
        ),
        go.Bar(
            name='Phase 2 (Pre-Commit)',
            x=['Phase 2'],
            y=[lat['phase2_avg']],
            marker_color='#9b59b6'
        ),
        go.Bar(
            name='Phase 3 (Commit)',
            x=['Phase 3'],
            y=[lat['phase3_avg']],
            marker_color='#2ecc71'
        )
    ])

    fig.update_layout(
        title='Average Phase Latency',
        yaxis_title='Latency (ms)',
        paper_bgcolor='rgba(0,0,0,0)',
        plot_bgcolor='rgba(0,0,0,0)',
        font=dict(color='white'),
        showlegend=False,
        xaxis=dict(showgrid=False),
        yaxis=dict(gridcolor='#444')
    )

    return json.dumps(fig, cls=plotly.utils.PlotlyJSONEncoder)


@app.route('/api/charts/latency-trend')
def latency_trend_chart():
    """Latency over time line chart."""

    snapshot = fetch_metrics_from_leader()

    lat = snapshot['latency']

    phase1_data = lat.get('phase1_data', [])
    phase2_data = lat.get('phase2_data', [])
    phase3_data = lat.get('phase3_data', [])

    x_vals = list(range(len(phase1_data)))

    fig = go.Figure()

    fig.add_trace(go.Scatter(
        x=x_vals,
        y=phase1_data,
        mode='lines+markers',
        name='Phase 1',
        line=dict(color='#3498db', width=2),
        marker=dict(size=6)
    ))

    fig.add_trace(go.Scatter(
        x=x_vals,
        y=phase2_data,
        mode='lines+markers',
        name='Phase 2',
        line=dict(color='#9b59b6', width=2),
        marker=dict(size=6)
    ))

    fig.add_trace(go.Scatter(
        x=x_vals,
        y=phase3_data,
        mode='lines+markers',
        name='Phase 3',
        line=dict(color='#2ecc71', width=2),
        marker=dict(size=6)
    ))

    fig.update_layout(
        title='Latency Trend (Last 20 Transactions)',
        xaxis_title='Transaction #',
        yaxis_title='Latency (ms)',
        paper_bgcolor='rgba(0,0,0,0)',
        plot_bgcolor='rgba(0,0,0,0)',
        font=dict(color='white'),
        xaxis=dict(showgrid=False),
        yaxis=dict(gridcolor='#444'),
        hovermode='x unified'
    )

    return json.dumps(fig, cls=plotly.utils.PlotlyJSONEncoder)


def run_dashboard(host='0.0.0.0', port=8000):
    """Run the dashboard server."""

    print(f"\n{'='*70}")
    print(f" 3PC Metrics Dashboard Starting")
    print(f"{'='*70}")
    print(f"Open in browser: http://localhost:{port}")
    print(f"{'='*70}\n")

    app.run(
        host=host,
        port=port,
        debug=True,
        use_reloader=False
    )


if __name__ == '__main__':
    run_dashboard()