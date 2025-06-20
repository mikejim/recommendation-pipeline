import dash
from dash import dcc, html
from dash.dependencies import Input, Output
import pandas as pd
import plotly.express as px
import os
import glob

PARQUET_DIR = "/app/shared_volume/parquet_output"
#PARQUET_DIR = "./shared_volume/parquet_output"
#PARQUET_DIR = "./data/parquet/watch"

app = dash.Dash(__name__)

app.layout = html.Div([
    html.H1("📊 Watch Time Analytics Dashboard", style={'textAlign': 'center', 'marginBottom': '30px'}),
    
    # Project Description Section
    html.Div([
        html.Div([
            html.H3("Project Overview", style={'color': '#2c3e50', 'marginBottom': '15px'}),
            html.P([
                "This dashboard provides comprehensive analytics for tracking and visualizing watch time patterns across different content categories. "
                "The system monitors viewing habits in real-time, offering insights into content consumption trends, genre preferences, "
                "and temporal viewing patterns to help optimize content strategy and user engagement."
            ], style={
                'fontSize': '16px',
                'lineHeight': '1.6',
                'color': '#34495e',
                'marginBottom': '20px',
                'textAlign': 'justify'
            }),
            html.P([
                "The data used in this dashboard is sourced from a streaming pipeline that processes watch events in real-time. ",
                "Parquet files are generated every 30 seconds, capturing user interactions with various shows. ",
                "Spark is used to process these events, aggregating watch time by genre and time of day. ",
                "Kafka is utilized for real-time data ingestion, while Redis is employed for fast access to user-specific watch events. ",
                "For more details on the data processing pipeline, refer to the project repository.",
                html.A("https://github.com/mikejim/recommendation-pipeline", href="https://github.com/mikejim/recommendation-pipeline", target="_blank")
            ], 
                style={
                'fontSize': '16px',
                'lineHeight': '1.6',
                'color': '#34495e',
                'marginBottom': '20px',
                'textAlign': 'justify'
            }),
            html.P([
                "📈 Real-time data updates every 30 seconds | ",
                "📊 Interactive visualizations | ",
                "🎬 Genre-based analytics | ",
                "⏰ Time-based insights"
            ], style={
                'fontSize': '14px',
                'color': '#7f8c8d',
                'fontStyle': 'italic',
                'textAlign': 'center',
                'padding': '10px',
                'backgroundColor': '#ecf0f1',
                'borderRadius': '5px'
            })
        ], style={
            'padding': '20px',
            'backgroundColor': '#f8f9fa',
            'borderRadius': '8px',
            'marginBottom': '30px',
            'border': '1px solid #e9ecef'
        })
    ]),
    
    dcc.Interval(id="interval", interval=30000, n_intervals=0),
    
    # Row 1: Time series charts
    html.Div([
        html.Div([
            dcc.Graph(id="area-chart")
        ], className="six columns"),
        html.Div([
            dcc.Graph(id="cumulative-chart")
        ], className="six columns"),
    ], className="row", style={'marginBottom': '20px'}),
    
    # Row 2: Summary charts
    html.Div([
        html.Div([
            dcc.Graph(id="genre-bar-chart")
        ], className="six columns"),
        html.Div([
            dcc.Graph(id="heatmap-chart")
        ], className="six columns"),
    ], className="row"),
    

])

def safe_read_parquet(path):
    try:
        return pd.read_parquet(path)
    except Exception as e:
        print(f"[dash] ⚠️ Skipping file: {path} ({e})")
        return pd.DataFrame()

# Multiple callbacks for different charts
@app.callback(
    [Output("area-chart", "figure"),
     Output("cumulative-chart", "figure"),
     Output("genre-bar-chart", "figure"),
     Output("heatmap-chart", "figure")],
    Input("interval", "n_intervals")
)
def update_all_graphs(n):
    print(f"[dash] 🚀 Callback triggered! Interval: {n}")
    
    # Get the processed data
    df_grouped, df_raw = get_processed_data()
    
    if df_grouped is None or df_raw is None:
        # Return empty charts if no data
        empty_fig = px.line(title="No data available")
        return empty_fig, empty_fig, empty_fig, empty_fig
    
    # 1. Stacked area chart - shows total watch time and genre distribution over time
    area_fig = px.area(df_grouped, x="event_time", y="duration_watched", color="genre",
                       title="📈 Watch Time Distribution by Genre Over Time")
    area_fig.update_layout(height=400, margin={"t": 50, "l": 30, "r": 30})
    
    # 2. Cumulative line chart
    df_cumulative = df_grouped.copy()
    df_cumulative['cumulative_watch_time'] = df_cumulative.groupby('genre')['duration_watched'].cumsum()
    cumulative_fig = px.line(df_cumulative, x="event_time", y="cumulative_watch_time", color="genre",
                            title="📊 Cumulative Watch Time by Genre", markers=True)
    cumulative_fig.update_layout(height=400, margin={"t": 50, "l": 30, "r": 30})
    
    # 3. Bar chart showing total watch time per genre (aggregated)
    genre_totals = df_raw.groupby("genre")["duration_watched"].sum().reset_index()
    genre_totals = genre_totals.sort_values("duration_watched", ascending=False)
    bar_fig = px.bar(genre_totals, x="genre", y="duration_watched",
                     title="🏆 Total Watch Time by Genre",
                     color="duration_watched", color_continuous_scale="viridis")
    bar_fig.update_layout(height=400, margin={"t": 50, "l": 30, "r": 30})
    
    # 4. Heatmap showing watch patterns by hour and genre
    try:
        df_heatmap = df_raw.copy()
        df_heatmap['hour'] = df_heatmap['event_time'].dt.hour
        heatmap_data = df_heatmap.groupby(['hour', 'genre'])['duration_watched'].sum().reset_index()
        
        if len(heatmap_data) > 0:
            # Create pivot table for heatmap
            pivot_data = heatmap_data.pivot(index='genre', columns='hour', values='duration_watched').fillna(0)
            heatmap_fig = px.imshow(pivot_data, 
                                   title="🕐 Watch Time Heatmap by Hour and Genre",
                                   labels=dict(x="Hour of Day", y="Genre", color="Watch Time"),
                                   aspect="auto")
        else:
            heatmap_fig = px.imshow([[0]], title="🕐 Watch Time Heatmap (No data)")
    except Exception as e:
        print(f"[dash] ⚠️ Error creating heatmap: {e}")
        heatmap_fig = px.line(title="🕐 Heatmap Error - Not enough data")
    
    heatmap_fig.update_layout(height=400, margin={"t": 50, "l": 30, "r": 30})
    
    print(f"[dash] ✅ All graphs updated successfully")
    return area_fig, cumulative_fig, bar_fig, heatmap_fig

def get_processed_data():
    """Extract data processing logic into a separate function"""
def get_processed_data():
    """Extract data processing logic into a separate function"""
    # Check if directory exists
    if not os.path.exists(PARQUET_DIR):
        print(f"[dash] ❌ Directory does not exist: {PARQUET_DIR}")
        return None, None
    
    print(f"[dash] 🔍 Looking in directory: {PARQUET_DIR}")
    
    # Find all parquet files - use a more specific pattern
    parquet_pattern = os.path.join(PARQUET_DIR, "**", "*.parquet")
    print(f"[dash] 🔍 Using pattern: {parquet_pattern}")
    
    files = glob.glob(parquet_pattern, recursive=True)
    #print(f"[dash] 📁 Raw glob results: {files}")
    
    # Filter to ensure we only have actual parquet files
    valid_files = []
    for f in files:
        if isinstance(f, str) and os.path.isfile(f) and f.endswith('.parquet'):
            valid_files.append(f)
            #print(f"[dash] ✅ Valid parquet file: {f}")
        else:
            print(f"[dash] ❌ Invalid file: {f} (type: {type(f)}, exists: {os.path.exists(f) if isinstance(f, str) else 'N/A'})")
    
    if not valid_files:
        print(f"[dash] ℹ️ No valid parquet files found in {PARQUET_DIR}")
        # List directory contents for debugging
        try:
            contents = os.listdir(PARQUET_DIR)
            #print(f"[dash] 📂 Directory contents: {contents}")
            
            # Check subdirectories
            for item in contents:
                item_path = os.path.join(PARQUET_DIR, item)
                if os.path.isdir(item_path):
                    sub_contents = os.listdir(item_path)
                    #print(f"[dash] 📂 Subdirectory {item}: {sub_contents}")
        except Exception as e:
            print(f"[dash] ❌ Cannot list directory: {e}")
        
        return None, None

    # Read and combine all parquet files
    try:
        dfs = []
        for f in valid_files:
            df = safe_read_parquet(f)
            if not df.empty:
                dfs.append(df)
                #print(f"[dash] ✅ Loaded {len(df)} rows from {f}")
        
        if not dfs:
            return None, None
            
        df = pd.concat(dfs, ignore_index=True)
        print(f"[dash] 🧮 Total loaded: {len(df)} rows")
        print(f"[dash] 📋 Columns: {df.columns.tolist()}")
        
        # Show a sample of the data for debugging
        if len(df) > 0:
            print(f"[dash] 📊 Sample data:\n{df.head()}")
            
    except Exception as e:
        print(f"[dash] ❌ Failed to load/combine parquet files: {e}")
        return None, None
 
    if df.empty:
        return None, None

    # Ensure required columns exist
    required_cols = ["event_time", "genre", "duration_watched"]
    missing_cols = [col for col in required_cols if col not in df.columns]
    if missing_cols:
        print(f"[dash] ❌ Missing required columns: {missing_cols}")
        return None, None

    # Clean and process data
    df = df.dropna(subset=required_cols)
    
    # Convert event_time to datetime if it's not already
    if not pd.api.types.is_datetime64_any_dtype(df['event_time']):
        df['event_time'] = pd.to_datetime(df['event_time'])
    
    # Group by time and genre for time series data
    df_grouped = df.groupby([pd.Grouper(key="event_time", freq="1min"), "genre"])["duration_watched"].sum().reset_index()
    print(f"[dash] 📊 Grouped data: {len(df_grouped)} rows")

    if df_grouped.empty:
        return None, None

    return df_grouped, df

if __name__ == "__main__":
    app.run(host="0.0.0.0", port=8050, debug=True)