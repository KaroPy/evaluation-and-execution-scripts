# Approach: Statistical Bucket Definition

## Core Idea

Instead of fixed percentages (10%, 20%, 30%), derive thresholds from the actual conversion probability distribution per product:

```python
import numpy as np
from scipy import stats

def define_audience_buckets(df, product_col, prob_col, timestamp_col):
    """
    Define audience buckets based on conversion probability distribution.

    Returns thresholds for:
    - Premium: Top performers (high probability, high confidence)
    - Growth: Mid-tier with upside potential
    - Volume: Broader reach, lower probability
    """
    results = {}

    for product in df[product_col].unique():
        product_data = df[df[product_col] == product]
        probs = product_data[prob_col].dropna()

        # 1. Basic percentile approach
        p90 = np.percentile(probs, 90)  # Premium threshold
        p80 = np.percentile(probs, 80)  # Growth threshold
        p70 = np.percentile(probs, 70)  # Volume threshold

        # 2. Adjust based on distribution shape
        skewness = stats.skew(probs)
        kurtosis = stats.kurtosis(probs)

        # 3. Confidence adjustment based on sample size
        n_samples = len(probs)
        confidence = min(1.0, n_samples / 5000)  # Scale to data volume

        # 4. Recency weighting - recent data more reliable
        recency_weight = calculate_recency_weight(product_data, timestamp_col)

        results[product] = {
            'premium_threshold': p90,
            'growth_threshold': p80,
            'volume_threshold': p70,
            'skewness': skewness,
            'confidence': confidence,
            'recency_weight': recency_weight,
            'n_samples': n_samples
        }

    return results
```

## Dynamic Threshold Adjustment

```python
def adjust_thresholds_by_distribution(probs, base_percentiles=(90, 80, 70)):
    """
    Adjust percentile thresholds based on probability distribution characteristics.

    - Right-skewed (many low probs): Tighten premium, widen volume
    - Left-skewed (many high probs): Widen premium, tighten volume
    - High variance: Use wider gaps between tiers
    """
    skewness = stats.skew(probs)
    variance = np.var(probs)

    # Base percentiles
    premium_pct, growth_pct, volume_pct = base_percentiles

    # Skewness adjustment (±5 percentile points max)
    skew_adjustment = np.clip(skewness * 2, -5, 5)

    # Variance adjustment - high variance = wider tiers
    var_adjustment = np.clip(variance * 10, 0, 5)

    adjusted = {
        'premium': premium_pct + skew_adjustment,
        'growth': growth_pct + skew_adjustment - var_adjustment,
        'volume': volume_pct + skew_adjustment - var_adjustment * 2
    }

    return adjusted
```

## Implementation for Your Data

```python
def create_audience_segments(conversions_df, conversion_probs_df):
    """
    Create audience segments per product category.
    """
    # Merge conversion history with probabilities
    df = conversions_df.merge(conversion_probs_df, on='sessionId')

    audiences = {
        'premium': [],   # Top tier - highest LTV
        'growth': [],    # Mid tier - persuadable
        'volume': []     # Broad reach
    }

    for product in df['properties.products.name'].unique():
        product_df = df[df['properties.products.name'] == product]

        # Get probability distribution stats
        probs = product_df['conversion_probability']
        thresholds = adjust_thresholds_by_distribution(probs)

        # Calculate actual cutoff values
        premium_cutoff = np.percentile(probs, thresholds['premium'])
        growth_cutoff = np.percentile(probs, thresholds['growth'])
        volume_cutoff = np.percentile(probs, thresholds['volume'])

        # Segment users
        audiences['premium'].extend(
            product_df[probs >= premium_cutoff]['anonymousId'].tolist()
        )
        audiences['growth'].extend(
            product_df[(probs >= growth_cutoff) & (probs < premium_cutoff)]['anonymousId'].tolist()
        )
        audiences['volume'].extend(
            product_df[(probs >= volume_cutoff) & (probs < growth_cutoff)]['anonymousId'].tolist()
        )

    return audiences
```

## Visualization to Validate Thresholds

```python
def visualize_bucket_distribution(df, product, prob_col):
    """Visualize where thresholds fall on the probability distribution."""
    import matplotlib.pyplot as plt

    probs = df[df['properties.products.name'] == product][prob_col]
    thresholds = adjust_thresholds_by_distribution(probs)

    fig, ax = plt.subplots(figsize=(10, 6))

    # Histogram of probabilities
    ax.hist(probs, bins=50, alpha=0.7, edgecolor='black')

    # Add threshold lines
    colors = {'premium': 'gold', 'growth': 'green', 'volume': 'blue'}
    for tier, pct in thresholds.items():
        cutoff = np.percentile(probs, pct)
        ax.axvline(cutoff, color=colors[tier], linestyle='--',
                   label=f'{tier}: {pct:.0f}th pct (prob={cutoff:.3f})')

    ax.set_xlabel('Conversion Probability')
    ax.set_ylabel('Count')
    ax.set_title(f'Audience Buckets for {product}')
    ax.legend()
    plt.show()
```

## Summary Table

| Audience | Default Percentile | Adjusted By | Purpose |
|----------|-------------------|-------------|---------|
| Premium | Top 10% (90th) | +skew, +confidence | High-value retargeting, lookalikes |
| Growth | 10-20% (80-90th) | +skew, -variance | Conversion campaigns |
| Volume | 20-30% (70-80th) | +skew, -variance×2 | Awareness, broad reach |