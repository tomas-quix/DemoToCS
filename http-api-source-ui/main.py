import os
import json
import threading
import time
from collections import deque
from flask import Flask, request, Response, redirect, render_template_string, stream_with_context
from flasgger import Swagger
from waitress import serve
from flask_cors import CORS
from setup_logging import get_logger
from quixstreams import Application
from dotenv import load_dotenv

load_dotenv()

service_url = os.environ["Quix__Deployment__Network__PublicUrl"]

quix_app = Application()
output_topic = quix_app.topic(os.environ["output"])
producer = quix_app.get_producer()

# Low-level consumer for the magnetometer dashboard
input_topic_name = os.environ.get("input", "table-data")
MAX_POINTS = 200
data_buffer = deque(maxlen=MAX_POINTS)
data_lock = threading.Lock()

logger = get_logger()


def consume_messages():
    """Background thread: consumes magnetometer messages from the input topic."""
    with quix_app.get_consumer() as consumer:
        consumer.subscribe([input_topic_name])
        while True:
            msg = consumer.poll(1.0)
            if msg is None:
                continue
            if msg.error():
                logger.error(f"Consumer error: {msg.error()}")
                continue
            try:
                value = json.loads(msg.value())
                if all(k in value for k in ("magnetometer-x", "magnetometer-y", "magnetometer-z")):
                    with data_lock:
                        data_buffer.append(value)
                    consumer.store_offsets(message=msg)
            except Exception as e:
                logger.error(f"Error processing message: {e}")


consumer_thread = threading.Thread(target=consume_messages, daemon=True)
consumer_thread.start()

app = Flask(__name__)
CORS(app)

app.config['SWAGGER'] = {
    'title': 'HTTP API Source',
    'description': 'Test your HTTP API with this Swagger interface. Send data and see it arrive in Quix.',
    'uiversion': 3
}
swagger = Swagger(app)

DASHBOARD_HTML = r"""<!DOCTYPE html>
<html lang="en">
<head>
    <meta charset="UTF-8">
    <meta name="viewport" content="width=device-width, initial-scale=1.0">
    <title>Magnetometer Dashboard</title>
    <script src="https://cdn.jsdelivr.net/npm/chart.js@4.4.0/dist/chart.umd.min.js"></script>
    <script src="https://cdn.jsdelivr.net/npm/luxon@3.4.4/build/global/luxon.min.js"></script>
    <script src="https://cdn.jsdelivr.net/npm/chartjs-adapter-luxon@1.3.1/dist/chartjs-adapter-luxon.umd.min.js"></script>
    <style>
        * { box-sizing: border-box; margin: 0; padding: 0; }
        body {
            background: #0f1117;
            color: #e0e0e0;
            font-family: 'Segoe UI', system-ui, sans-serif;
            min-height: 100vh;
        }
        header {
            background: #1a1d27;
            border-bottom: 1px solid #2a2d3a;
            padding: 16px 24px;
            display: flex;
            align-items: center;
            gap: 12px;
        }
        header h1 { font-size: 1.25rem; font-weight: 600; color: #ffffff; }
        .dot {
            width: 10px; height: 10px;
            border-radius: 50%;
            background: #22c55e;
            animation: pulse 1.5s infinite;
        }
        .dot.disconnected { background: #ef4444; animation: none; }
        @keyframes pulse {
            0%, 100% { opacity: 1; }
            50% { opacity: 0.3; }
        }
        .status-text { font-size: 0.8rem; color: #6b7280; margin-left: auto; }
        .container { padding: 24px; }
        .cards {
            display: grid;
            grid-template-columns: repeat(3, 1fr);
            gap: 16px;
            margin-bottom: 24px;
        }
        .card {
            background: #1a1d27;
            border: 1px solid #2a2d3a;
            border-radius: 12px;
            padding: 20px;
        }
        .card-label {
            font-size: 0.75rem;
            text-transform: uppercase;
            letter-spacing: 0.05em;
            color: #6b7280;
            margin-bottom: 8px;
        }
        .card-value {
            font-size: 2rem;
            font-weight: 700;
            font-variant-numeric: tabular-nums;
        }
        .card-unit { font-size: 0.85rem; color: #6b7280; margin-left: 4px; }
        .card.x .card-value { color: #f87171; }
        .card.y .card-value { color: #4ade80; }
        .card.z .card-value { color: #60a5fa; }
        .chart-card {
            background: #1a1d27;
            border: 1px solid #2a2d3a;
            border-radius: 12px;
            padding: 20px;
        }
        .chart-card h2 {
            font-size: 0.9rem;
            font-weight: 500;
            color: #9ca3af;
            margin-bottom: 16px;
        }
        canvas { max-height: 380px; }
        @media (max-width: 600px) {
            .cards { grid-template-columns: 1fr; }
        }
    </style>
</head>
<body>
    <header>
        <div class="dot" id="statusDot"></div>
        <h1>Magnetometer Dashboard</h1>
        <span class="status-text" id="statusText">Connecting...</span>
    </header>
    <div class="container">
        <div class="cards">
            <div class="card x">
                <div class="card-label">Magnetometer X</div>
                <div class="card-value" id="valX">&#8212;<span class="card-unit">&#181;T</span></div>
            </div>
            <div class="card y">
                <div class="card-label">Magnetometer Y</div>
                <div class="card-value" id="valY">&#8212;<span class="card-unit">&#181;T</span></div>
            </div>
            <div class="card z">
                <div class="card-label">Magnetometer Z</div>
                <div class="card-value" id="valZ">&#8212;<span class="card-unit">&#181;T</span></div>
            </div>
        </div>
        <div class="chart-card">
            <h2>Magnetometer Over Time</h2>
            <canvas id="magChart"></canvas>
        </div>
    </div>
    <script>
    const ctx = document.getElementById('magChart').getContext('2d');
    const chart = new Chart(ctx, {
        type: 'line',
        data: {
            datasets: [
                {
                    label: 'X',
                    data: [],
                    borderColor: '#f87171',
                    backgroundColor: 'rgba(248,113,113,0.08)',
                    borderWidth: 2,
                    pointRadius: 0,
                    tension: 0.3,
                    fill: true,
                },
                {
                    label: 'Y',
                    data: [],
                    borderColor: '#4ade80',
                    backgroundColor: 'rgba(74,222,128,0.08)',
                    borderWidth: 2,
                    pointRadius: 0,
                    tension: 0.3,
                    fill: true,
                },
                {
                    label: 'Z',
                    data: [],
                    borderColor: '#60a5fa',
                    backgroundColor: 'rgba(96,165,250,0.08)',
                    borderWidth: 2,
                    pointRadius: 0,
                    tension: 0.3,
                    fill: true,
                }
            ]
        },
        options: {
            responsive: true,
            animation: false,
            interaction: { mode: 'index', intersect: false },
            scales: {
                x: {
                    type: 'time',
                    time: {
                        unit: 'second',
                        displayFormats: { second: 'HH:mm:ss' }
                    },
                    ticks: { color: '#6b7280', maxTicksLimit: 8 },
                    grid: { color: '#1f2330' }
                },
                y: {
                    ticks: { color: '#6b7280' },
                    grid: { color: '#1f2330' },
                    title: { display: true, text: '\u03bcT', color: '#6b7280' }
                }
            },
            plugins: {
                legend: { labels: { color: '#9ca3af' } },
                tooltip: {
                    callbacks: {
                        label: ctx => `${ctx.dataset.label}: ${ctx.parsed.y.toFixed(3)} \u03bcT`
                    }
                }
            }
        }
    });

    const statusDot = document.getElementById('statusDot');
    const statusText = document.getElementById('statusText');
    let totalMsgs = 0;

    function updateCards(item) {
        const fmt = v => v.toFixed(3);
        document.getElementById('valX').innerHTML =
            fmt(item['magnetometer-x']) + '<span class="card-unit">\u03bcT</span>';
        document.getElementById('valY').innerHTML =
            fmt(item['magnetometer-y']) + '<span class="card-unit">\u03bcT</span>';
        document.getElementById('valZ').innerHTML =
            fmt(item['magnetometer-z']) + '<span class="card-unit">\u03bcT</span>';
    }

    function applySnapshot(items) {
        // Replace chart data with the full buffer snapshot
        chart.data.datasets[0].data = items.map(i => ({ x: i.time / 1e6, y: i['magnetometer-x'] }));
        chart.data.datasets[1].data = items.map(i => ({ x: i.time / 1e6, y: i['magnetometer-y'] }));
        chart.data.datasets[2].data = items.map(i => ({ x: i.time / 1e6, y: i['magnetometer-z'] }));
        chart.update();
        if (items.length > 0) {
            updateCards(items[items.length - 1]);
            totalMsgs = items.length;
            statusText.textContent = `Live \u00b7 ${totalMsgs} points`;
        }
    }

    function connect() {
        const es = new EventSource('/stream/magnetometer');

        es.onopen = () => {
            statusDot.classList.remove('disconnected');
            statusText.textContent = 'Connecting...';
        };

        es.onmessage = (e) => {
            const items = JSON.parse(e.data);
            applySnapshot(items);
        };

        es.onerror = () => {
            statusDot.classList.add('disconnected');
            statusText.textContent = 'Reconnecting...';
            es.close();
            setTimeout(connect, 3000);
        };
    }

    connect();
    </script>
</body>
</html>"""


@app.route("/", methods=['GET'])
def index():
    return redirect("/dashboard")


@app.route("/dashboard", methods=['GET'])
def dashboard():
    return render_template_string(DASHBOARD_HTML)


@app.route("/api/magnetometer", methods=['GET'])
def get_magnetometer_data():
    """Return the current magnetometer data buffer as JSON."""
    with data_lock:
        data = list(data_buffer)
    return Response(json.dumps(data), mimetype='application/json')


@app.route("/stream/magnetometer", methods=['GET'])
def stream_magnetometer():
    """Server-Sent Events endpoint: pushes a full snapshot of the buffer every 500 ms."""
    def generate():
        while True:
            with data_lock:
                snapshot = list(data_buffer)
            yield f"data: {json.dumps(snapshot)}\n\n"
            time.sleep(0.5)

    return Response(
        stream_with_context(generate()),
        mimetype='text/event-stream',
        headers={
            'Cache-Control': 'no-cache',
            'X-Accel-Buffering': 'no',
        }
    )


@app.route("/apidocs-redirect", methods=['GET'])
def apidocs_redirect():
    return redirect("/apidocs/")


@app.route("/data/", methods=['POST'])
def post_data_without_key():
    """
    Post data without key
    ---
    parameters:
      - in: body
        name: body
        schema:
          type: object
          properties:
            some_value:
              type: string
    responses:
      200:
        description: Data received successfully
    """
    data = request.json
    logger.debug(f"{data}")
    producer.produce(output_topic.name, json.dumps(data))
    return Response(status=200)


@app.route("/data/<key>", methods=['POST'])
def post_data_with_key(key: str):
    """
    Post data with a key
    ---
    parameters:
      - in: path
        name: key
        type: string
        required: true
      - in: body
        name: body
        schema:
          type: object
          properties:
            some_value:
              type: string
    responses:
      200:
        description: Data received successfully
    """
    data = request.json
    logger.debug(f"{data}")
    producer.produce(output_topic.name, json.dumps(data), key.encode())
    return Response(status=200)


if __name__ == '__main__':
    print("=" * 60)
    print(" " * 20 + "CURL EXAMPLE")
    print("=" * 60)
    print(
        f"""
curl -L -X POST \\
    -H 'Content-Type: application/json' \\
    -d '{{"key": "value"}}' \\
    {service_url}/data
    """
    )
    print("=" * 60)
    print(f"\n  Dashboard: {service_url}/dashboard\n")
    print("=" * 60)

    serve(app, host="0.0.0.0", port=80)
