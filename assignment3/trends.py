import pandas as pd
import matplotlib.pyplot as plt
import seaborn as sns
from statsmodels.tsa.stattools import adfuller
from prophet import Prophet
from assignment3.config import TRENDS_FILE, OUTPUT_DIR, ensure_dirs

class TrendsAnalyzer:
    def __init__(self, file_path=TRENDS_FILE):
        self.file_path = file_path
        self.df = None

    def load_and_clean(self):
        """Loads Google Trends CSV, skips metadata, and fixes columns."""
        print(f"Loading trends from {self.file_path}...")
        # Google Trends CSVs have 2 lines of metadata
        self.df = pd.read_csv(self.file_path, skiprows=2)
        
        # Rename columns: first is 'ds' (date), second is 'y' (value) for Prophet
        self.df.columns = ['ds', 'y']
        
        # Convert 'y' to numeric (sometimes it has '<1' which becomes 0)
        self.df['y'] = pd.to_numeric(self.df['y'].astype(str).replace('<1', '0'), errors='coerce').fillna(0)
        
        # Convert 'ds' to datetime
        self.df['ds'] = pd.to_datetime(self.df['ds'])
        
        print(f"Loaded {len(self.df)} months of data.")
        return self.df

    def check_stationarity(self):
        """Performs Augmented Dickey-Fuller test."""
        print("\n=== Stationarity Test (ADF) ===")
        result = adfuller(self.df['y'])
        print(f'ADF Statistic: {result[0]:.4f}')
        print(f'p-value: {result[1]:.4f}')
        
        is_stationary = result[1] < 0.05
        status = "Stationary" if is_stationary else "Non-Stationary (has trend/seasonality)"
        print(f"Result: {status}")
        
        with open(OUTPUT_DIR / "trends_stationarity.txt", "w") as f:
            f.write(f"ADF Statistic: {result[0]}\np-value: {result[1]}\nStatus: {status}\n")
        
        return is_stationary

    def plot_historical(self):
        """Plots the raw search interest."""
        plt.figure(figsize=(12, 6))
        sns.lineplot(data=self.df, x='ds', y='y', color='teal')
        plt.title("Search Interest for 'intern' in Kazakhstan (2004-2025)")
        plt.ylabel("Interest Score (0-100)")
        plt.xlabel("Year")
        plt.grid(True, alpha=0.3)
        plt.tight_layout()
        plt.savefig(OUTPUT_DIR / "trends_historical.png", dpi=180)
        plt.close()

    def run_forecast(self, periods=10):
        """Uses Prophet to forecast future interest."""
        print(f"\nForecasting next {periods} months...")
        model = Prophet(yearly_seasonality=True, weekly_seasonality=False, daily_seasonality=False)
        model.fit(self.df)
        
        future = model.make_future_dataframe(periods=periods, freq='ME')
        forecast = model.predict(future)
        
        # Plot forecast
        fig1 = model.plot(forecast)
        plt.title(f"Forecast for 'intern' search interest ({periods} months)")
        plt.xlabel("Date")
        plt.ylabel("Interest")
        plt.savefig(OUTPUT_DIR / "trends_forecast.png", dpi=180)
        plt.close()
        
        # Plot components (trend, yearly seasonality)
        fig2 = model.plot_components(forecast)
        plt.savefig(OUTPUT_DIR / "trends_components.png", dpi=180)
        plt.close()
        
        # Save forecast data
        forecast[['ds', 'yhat', 'yhat_lower', 'yhat_upper']].tail(periods + 5).to_csv(
            OUTPUT_DIR / "trends_forecast_data.csv", index=False
        )
        print(f"Forecast completed. Results saved to {OUTPUT_DIR}")
        return forecast

def main():
    ensure_dirs()
    analyzer = TrendsAnalyzer()
    analyzer.load_and_clean()
    analyzer.check_stationarity()
    analyzer.plot_historical()
    analyzer.run_forecast(periods=10)

if __name__ == "__main__":
    main()
