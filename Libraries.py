import pandas as pd
import numpy as np
import warnings, os, json, time, random, hashlib, itertools
from datetime import datetime, timedelta
from collections import defaultdict
from faker import Faker

import matplotlib.pyplot as plt
import matplotlib.patches as mpatches
import matplotlib.gridspec as gridspec
import seaborn as sns
import plotly.express as px
import plotly.graph_objects as go
from plotly.subplots import make_subplots

from sklearn.cluster import KMeans
from sklearn.preprocessing import StandardScaler, LabelEncoder
from sklearn.metrics import silhouette_score
from sklearn.linear_model import LogisticRegression
from sklearn.model_selection import train_test_split
from scipy import stats
import statsmodels.api as sm

from lifetimes import BetaGeoFitter, GammaGammaFitter
from lifetimes.plotting import (
    plot_frequency_recency_matrix,
    plot_probability_alive_matrix
)

import mlflow
import mlflow.sklearn
import duckdb

try:
    from pyspark.sql import SparkSession
    from pyspark.sql import functions as F
    from pyspark.sql.window import Window
    from pyspark.sql.types import *
    from delta import configure_spark_with_delta_pip
    SPARK_AVAILABLE = True
except Exception as e:
    SPARK_AVAILABLE = False
    print(f'  PySpark/Delta: {e} — will use DuckDB/Pandas equivalent')

warnings.filterwarnings('ignore')
np.random.seed(42)
random.seed(42)
fake = Faker()
Faker.seed(42)

DARK_BG  = '#0D1117'
CARD_BG  = '#161B22'
ACCENT   = '#00D4FF'
GREEN    = '#00FF87'
ORANGE   = '#FF6B35'
YELLOW   = '#FFD700'
PURPLE   = '#C084FC'
RED      = '#FF4444'
WHITE    = '#E6EDF3'
GRAY     = '#8B949E'

plt.rcParams.update({
    'figure.facecolor' : DARK_BG,
    'axes.facecolor'   : CARD_BG,
    'axes.edgecolor'   : '#30363D',
    'text.color'       : WHITE,
    'xtick.color'      : GRAY,
    'ytick.color'      : GRAY,
    'axes.labelcolor'  : GRAY,
    'axes.grid'        : True,
    'grid.alpha'       : 0.15,
    'grid.color'       : '#30363D',
    'font.family'      : 'DejaVu Sans',
})

os.makedirs('/content/lakehouse/bronze',  exist_ok=True)
os.makedirs('/content/lakehouse/silver',  exist_ok=True)
os.makedirs('/content/lakehouse/gold',    exist_ok=True)
os.makedirs('/content/lakehouse/serving', exist_ok=True)
os.makedirs('/content/mlruns',            exist_ok=True)

print(' Imports complete | Theme set | Directories created')
print(f'   PySpark available: {SPARK_AVAILABLE}')