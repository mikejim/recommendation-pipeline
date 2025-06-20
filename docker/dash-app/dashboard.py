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
    html.H2("📊 Total Watch Time per Genre Over Time"),
    dcc.Interval(id="interval", interval=30000, n_intervals=0),
    dcc.Graph(id="watch-graph")
])

def safe_read_parquet(path):
    try:
        return pd.read_parquet(path)
    except Exception as e:
        print(f"[dash] ⚠️ Skipping file: {path} ({e})")
        return pd.DataFrame()

@app.callback(
    Output("watch-graph", "figure"),
    Input("interval", "n_intervals")
)
def update_graph(n):
    print(f"[dash] 🚀 Callback triggered! Interval: {n}")
    
    # Check if directory exists
    if not os.path.exists(PARQUET_DIR):
        print(f"[dash] ❌ Directory does not exist: {PARQUET_DIR}")
        return px.line(title="Data directory not found")
    
    print(f"[dash] 🔍 Looking in directory: {PARQUET_DIR}")
    
    # Find all parquet files - use a more specific pattern
    parquet_pattern = os.path.join(PARQUET_DIR, "**", "*.parquet")
    print(f"[dash] 🔍 Using pattern: {parquet_pattern}")
    
    files = glob.glob(parquet_pattern, recursive=True)
    print(f"[dash] 📁 Raw glob results: {files}")
    
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
        
        return px.line(title="No parquet files found")

    # Read and combine all parquet files
    try:
        dfs = []
        for f in valid_files:
            df = safe_read_parquet(f)
            if not df.empty:
                dfs.append(df)
                #print(f"[dash] ✅ Loaded {len(df)} rows from {f}")
        
        if not dfs:
            return px.line(title="All parquet files are empty")
            
        df = pd.concat(dfs, ignore_index=True)
        print(f"[dash] 🧮 Total loaded: {len(df)} rows")
        print(f"[dash] 📋 Columns: {df.columns.tolist()}")
        
        # Show a sample of the data for debugging
        if len(df) > 0:
            print(f"[dash] 📊 Sample data:\n{df.head()}")
            
    except Exception as e:
        print(f"[dash] ❌ Failed to load/combine parquet files: {e}")
        return px.line(title="Failed to load data")
 
    if df.empty:
        return px.line(title="No data in parquet files")

    # Ensure required columns exist
    required_cols = ["event_time", "genre", "duration_watched"]
    missing_cols = [col for col in required_cols if col not in df.columns]
    if missing_cols:
        print(f"[dash] ❌ Missing required columns: {missing_cols}")
        return px.line(title=f"Missing columns: {missing_cols}")

    # Clean and process data
    df = df.dropna(subset=required_cols)
    
    # Convert event_time to datetime if it's not already
    if not pd.api.types.is_datetime64_any_dtype(df['event_time']):
        df['event_time'] = pd.to_datetime(df['event_time'])
    
    # Group by time and genre
    df_grouped = df.groupby([pd.Grouper(key="event_time", freq="1min"), "genre"])["duration_watched"].sum().reset_index()
    print(f"[dash] 📊 Grouped data: {len(df_grouped)} rows")

    if df_grouped.empty:
        return px.line(title="No data after grouping")

    # Create the plot
    fig = px.line(df_grouped, x="event_time", y="duration_watched", color="genre",
                  title="Total Watch Time per Genre (1-min window)", markers=True)
    fig.update_layout(height=600, margin={"t": 50, "l": 30, "r": 30})
    
    print(f"[dash] ✅ Graph created successfully")
    return fig

if __name__ == "__main__":
    app.run(host="0.0.0.0", port=8050, debug=True)
