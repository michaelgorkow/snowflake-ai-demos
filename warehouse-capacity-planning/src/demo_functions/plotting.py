import pandas as pd
import numpy as np
import plotly.graph_objects as go
from plotly.subplots import make_subplots


def plot_utilization_timeseries(df: pd.DataFrame, warehouse_ids: list = None) -> go.Figure:
    if warehouse_ids is not None:
        df = df[df['WAREHOUSE_ID'].isin(warehouse_ids)]

    fig = go.Figure()
    for wh_id in df['WAREHOUSE_ID'].unique():
        wh_data = df[df['WAREHOUSE_ID'] == wh_id].sort_values('DATE')
        fig.add_trace(go.Scatter(
            x=wh_data['DATE'], y=wh_data['UTILIZATION_PCT'],
            mode='lines', name=wh_id, opacity=0.8,
        ))

    fig.update_layout(
        title='Warehouse Utilization Over Time',
        xaxis_title='Date', yaxis_title='Utilization %',
        yaxis=dict(tickformat='.0%'),
        template='plotly_white', height=500,
    )
    return fig


def plot_seasonal_patterns(df: pd.DataFrame) -> go.Figure:
    df = df.copy()
    df['MONTH'] = pd.to_datetime(df['DATE']).dt.month
    df['DAY_OF_WEEK'] = pd.to_datetime(df['DATE']).dt.dayofweek

    fig = make_subplots(rows=1, cols=2, subplot_titles=['By Month', 'By Day of Week'])

    for m in range(1, 13):
        month_data = df[df['MONTH'] == m]['UTILIZATION_PCT']
        fig.add_trace(go.Box(y=month_data, name=str(m), showlegend=False), row=1, col=1)

    day_names = ['Mon', 'Tue', 'Wed', 'Thu', 'Fri', 'Sat', 'Sun']
    for d in range(7):
        day_data = df[df['DAY_OF_WEEK'] == d]['UTILIZATION_PCT']
        fig.add_trace(go.Box(y=day_data, name=day_names[d], showlegend=False), row=1, col=2)

    fig.update_layout(
        title='Seasonal Utilization Patterns',
        template='plotly_white', height=450,
    )
    fig.update_yaxes(tickformat='.0%')
    return fig


def plot_correlation_heatmap(df: pd.DataFrame) -> go.Figure:
    numeric_cols = df.select_dtypes(include=[np.number]).columns.tolist()
    cols_to_exclude = ['DAY_OF_WEEK']
    numeric_cols = [c for c in numeric_cols if c not in cols_to_exclude]

    corr = df[numeric_cols].corr()

    fig = go.Figure(data=go.Heatmap(
        z=corr.values,
        x=corr.columns.tolist(),
        y=corr.index.tolist(),
        colorscale='RdBu_r', zmid=0,
        text=np.round(corr.values, 2),
        texttemplate='%{text}',
        textfont=dict(size=9),
    ))

    fig.update_layout(
        title='Feature Correlation Heatmap',
        template='plotly_white', height=600, width=700,
    )
    return fig


def plot_utilization_distribution(df: pd.DataFrame) -> go.Figure:
    fig = make_subplots(rows=1, cols=2, subplot_titles=['By Region', 'By Warehouse Type'])

    for region in df['REGION'].unique():
        region_data = df[df['REGION'] == region]['UTILIZATION_PCT']
        fig.add_trace(go.Violin(
            y=region_data, name=region, box_visible=True,
            meanline_visible=True, showlegend=False,
        ), row=1, col=1)

    for wh_type in df['WAREHOUSE_TYPE'].unique():
        type_data = df[df['WAREHOUSE_TYPE'] == wh_type]['UTILIZATION_PCT']
        fig.add_trace(go.Violin(
            y=type_data, name=wh_type, box_visible=True,
            meanline_visible=True, showlegend=False,
        ), row=1, col=2)

    fig.update_layout(
        title='Utilization Distribution',
        template='plotly_white', height=450,
    )
    fig.update_yaxes(tickformat='.0%')
    return fig


def plot_feature_importance(feature_names: list, importances: list) -> go.Figure:
    fig = go.Figure(go.Bar(
        x=importances, y=feature_names,
        orientation='h',
        marker_color='steelblue',
    ))

    fig.update_layout(
        title='Feature Importance (LightGBM)',
        xaxis_title='Importance',
        template='plotly_white', height=max(400, len(feature_names) * 22),
    )
    return fig


def plot_actual_vs_predicted(actual: pd.Series, predicted: pd.Series,
                             dates: pd.Series = None) -> go.Figure:
    fig = go.Figure()

    x = dates if dates is not None else list(range(len(actual)))

    fig.add_trace(go.Scatter(
        x=x, y=actual, mode='markers', name='Actual',
        marker=dict(size=3, opacity=0.4),
    ))
    fig.add_trace(go.Scatter(
        x=x, y=predicted, mode='markers', name='Predicted',
        marker=dict(size=3, opacity=0.4),
    ))

    fig.update_layout(
        title='Actual vs Predicted Utilization',
        xaxis_title='Date' if dates is not None else 'Index',
        yaxis_title='Utilization %',
        yaxis=dict(tickformat='.0%'),
        template='plotly_white', height=450,
    )
    return fig


def plot_forecast(historical_df: pd.DataFrame, forecast_df: pd.DataFrame,
                  warehouse_ids: list = None) -> go.Figure:
    if warehouse_ids is not None:
        historical_df = historical_df[historical_df['WAREHOUSE_ID'].isin(warehouse_ids)]
        forecast_df = forecast_df[forecast_df['WAREHOUSE_ID'].isin(warehouse_ids)]

    fig = go.Figure()
    colors = ['#1f77b4', '#ff7f0e', '#2ca02c', '#d62728', '#9467bd']

    for i, wh_id in enumerate(historical_df['WAREHOUSE_ID'].unique()):
        color = colors[i % len(colors)]
        hist = historical_df[historical_df['WAREHOUSE_ID'] == wh_id].sort_values('DATE')
        fig.add_trace(go.Scatter(
            x=hist['DATE'], y=hist['UTILIZATION_PCT'],
            mode='lines', name=f'{wh_id} (actual)',
            line=dict(color=color), opacity=0.7,
        ))

        if wh_id in forecast_df['WAREHOUSE_ID'].values:
            fcst = forecast_df[forecast_df['WAREHOUSE_ID'] == wh_id].sort_values('DATE')
            fig.add_trace(go.Scatter(
                x=fcst['DATE'], y=fcst['FORECAST'],
                mode='lines', name=f'{wh_id} (forecast)',
                line=dict(color=color, dash='dash'), opacity=0.9,
            ))

    fig.update_layout(
        title='Historical Actuals vs Forecast',
        xaxis_title='Date', yaxis_title='Utilization %',
        yaxis=dict(tickformat='.0%'),
        template='plotly_white', height=500,
    )
    return fig


def plot_warehouse_attributes_timeline(df: pd.DataFrame, warehouse_ids: list = None) -> go.Figure:
    if warehouse_ids is not None:
        df = df[df['WAREHOUSE_ID'].isin(warehouse_ids)]

    fig = make_subplots(
        rows=3, cols=1, shared_xaxes=True,
        subplot_titles=['Total Capacity (sqm)', 'Loading Docks', 'Staffing Level'],
        vertical_spacing=0.08,
    )

    colors = ['#1f77b4', '#ff7f0e', '#2ca02c', '#d62728', '#9467bd']

    for i, wh_id in enumerate(df['WAREHOUSE_ID'].unique()):
        color = colors[i % len(colors)]
        wh_data = df[df['WAREHOUSE_ID'] == wh_id].sort_values('EFFECTIVE_DATE')

        fig.add_trace(go.Scatter(
            x=wh_data['EFFECTIVE_DATE'], y=wh_data['TOTAL_CAPACITY_SQM'],
            mode='lines+markers', name=wh_id, line=dict(color=color, shape='hv'),
            legendgroup=wh_id, showlegend=True,
        ), row=1, col=1)

        fig.add_trace(go.Scatter(
            x=wh_data['EFFECTIVE_DATE'], y=wh_data['NUM_LOADING_DOCKS'],
            mode='lines+markers', name=wh_id, line=dict(color=color, shape='hv'),
            legendgroup=wh_id, showlegend=False,
        ), row=2, col=1)

        fig.add_trace(go.Scatter(
            x=wh_data['EFFECTIVE_DATE'], y=wh_data['STAFFING_LEVEL'],
            mode='lines+markers', name=wh_id, line=dict(color=color, shape='hv'),
            legendgroup=wh_id, showlegend=False,
        ), row=3, col=1)

    fig.update_layout(
        title='Warehouse Attribute Changes Over Time',
        template='plotly_white', height=700,
    )
    return fig