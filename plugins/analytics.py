from sqlalchemy import create_engine, text
from sqlalchemy.orm import sessionmaker
import configparser
import os
import pandas as pd
import plotly.graph_objects as go
from plotly.subplots import make_subplots
import sys 
sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

# Load credentials from config
parser = configparser.ConfigParser()
parser.read(os.path.join(os.path.dirname(__file__), '../config/.conf'))


class Dashboard:
    def __init__(self):
        self.session = self.initialize_session()
        self.bar_trace = self.bar_chart()
        self.line_trace = self.line_chart()
        self.scatter_trace = self.scatter_plot()
    @staticmethod
    def initialize_session():
        psql_username = parser.get('Database', 'POSTGRES_USERNAME')
        psql_password = parser.get('Database', 'POSTGRES_PASSWORD')
        psql_connection_str = f'postgresql://{psql_username}:{psql_password}@localhost:5432/postgres'

        try:
            engine = create_engine(psql_connection_str)
            session_object = sessionmaker(bind=engine)
            session = session_object()
            print('Session initialized successfully')
            return session
        except Exception as e:
            print("Error initializing session:", e)
            return None

    def bar_chart(self):
        platforms = ['aws', 'azure', 'gcp', 'databricks', 'snowflake']
        values = {}
        for platform in platforms:
            query = text("""
                SELECT COUNT(*) FROM reddit_posts
                WHERE selftext ILIKE :platform
            """)
            result = self.session.execute(query, {"platform": f"%{platform}%"})
            count = result.scalar()
            values[platform] = count

        df_platforms = pd.DataFrame(list(values.items()), columns=['Platform', 'Mentions'])

        return go.Bar(
            x=df_platforms['Platform'],
            y=df_platforms['Mentions'],
            name="Platform Mentions",
            marker_color='rgb(158,202,225)',
            marker_line_color='rgb(8,48,107)',
            marker_line_width=1.5,
            opacity=0.6
        )

    def line_chart(self):
        query = text("""
            SELECT 
                EXTRACT(HOUR FROM TO_TIMESTAMP(created_utc, 'YYYY-MM-DD"T"HH24:MI:SS')) AS hour,
                AVG(score) AS avg_score
            FROM reddit_posts
            WHERE score IS NOT NULL AND created_utc IS NOT NULL
            GROUP BY hour
            ORDER BY hour;
        """)
        result = self.session.execute(query)
        df = pd.DataFrame(result.fetchall(), columns=["hour", "avg_score"])

        return go.Scatter(
            x=df['hour'],
            y=df['avg_score'],
            mode='lines+markers',
            name='Avg Score by Hour',
            line=dict(color='royalblue')
        )

    def scatter_plot(self):
        query = text("""
            SELECT title, score FROM reddit_posts
            WHERE title IS NOT NULL AND score IS NOT NULL
            LIMIT 1000;
        """)
        result = self.session.execute(query)
        df = pd.DataFrame(result.fetchall(), columns=["title", "score"])
        df['title_length'] = df['title'].str.len()

        return go.Scatter(
            x=df['title_length'],
            y=df['score'],
            mode='markers',
            name='Title Length vs Score',
            marker=dict(color='orange', size=8, opacity=0.6)
        )

    def subplots(self):
        fig = make_subplots(
            rows=2, cols=2,
            specs=[[{"type": "xy"}, {"type": "xy"}],
                   [{"colspan": 2, "type": "xy"}, None]],
            subplot_titles=[
                "Most Mentioned Platforms",
                "Avg Score by Hour",
                "Title Length vs Score"
            ]
        )

        fig.add_trace(self.bar_trace, row=1, col=1)
        fig.add_trace(self.line_trace, row=1, col=2)
        fig.add_trace(self.scatter_trace, row=2, col=1)

        fig.update_layout(
            height=800,
            title_text="Reddit Post Insights Dashboard",
            title_x=0.5,
            showlegend=False
        )
        
        fig.write_html('/opt/airflow/plugins/dashboards.html')
        
if __name__== '__main__':
    dashboard = Dashboard()
    dashboard.subplots()