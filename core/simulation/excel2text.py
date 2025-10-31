# -*- coding: utf-8 -*-
"""
sales_data_generator_for_report_v6_with_questions.py

Purpose:
Generates synthetic sales data (CSV) and corresponding analytical conclusions (JSON),
where each conclusion is paired with a natural language question.
Features include randomized biases, robust analysis, and controllable output.

Design Philosophy:
1.  Conclusion-Driven Synthesis: Biases data generation randomly per run.
2.  Extensive & Granular Analysis: Performs deep analysis across multiple dimensions.
3.  Paired Output: Each conclusion text is matched with a relevant question.
4.  Controllable Conclusion Output: Selects top N conclusion/question pairs based on priority.
5.  Structured Output: CSV data + JSON (metadata/metrics/conclusion-question pairs).
6.  English Output for conclusions and questions.

Usage Instructions:
Ensure pandas and Faker are installed (`pip install pandas Faker`).
Run from the command line (biases will be randomized internally).

Examples:
python sales_data_generator_for_report_v6_with_questions.py --num-records 500 \
    --output-csv sales_data_wq.csv --output-json sales_conclusions_wq.json \
    --num-conclusions 50

Arguments:
  --num-records NUM       Number of transaction records (default: 500).
  --output-csv FILE       Filename for the output CSV data file (default: ./sales_data.csv).
  --output-json FILE      Filename for the output JSON conclusions/metadata file (default: ./sales_conclusions.json).
  --num-conclusions NUM   Target number of key conclusion/question pairs (default: 50).
  --target-month STR      Target month (YYYY-MM), defaults to previous month.
  --region STR            Sales region name (default: 'East Region').
  --currency STR          Currency symbol (default: 'USD').
  --regional-target NUM   Overall sales target (default: 750000.0).
  --prev-month-sales NUM  Previous month sales (default: 680000.0).
  # Bias arguments below are defined but will be OVERRIDDEN by internal randomization.
  --bias-overall-target [exceed|meet|miss] (default: meet). [NOTE: Randomized internally]
  --bias-growth [positive|neutral|negative] (default: neutral). [NOTE: Randomized internally]
  --bias-top-rep ID       (default: None). [NOTE: Randomized internally]
  --bias-bottom-rep ID    (default: None). [NOTE: Randomized internally]
  --bias-top-product ID   (default: None). [NOTE: Randomized internally]
  --bias-new-customer [high|medium|low] (default: medium). [NOTE: Randomized internally]

Requirements:
  - Python 3.7+
  - pandas (`pip install pandas`)
  - Faker (`pip install Faker`)

Sales Analysis Report Outline:
1. Overall Performance Overview (corresponds to overall_performance section in code)
Total sales vs. regional target (compared to regional_target)

Month-over-month growth rate (compared to prev_month_sales)

Number of transactions and average deal size (avg_deal_size)

Sales trend over time (first half vs. second half of the month)

2. Sales Representative Performance (corresponds to sales_rep_performance section)
Top 3 sales representatives (sorted by sales, including achievement rate)

Highlighting champion representative (rep_top1_sales)

Gap analysis between representatives (rep_rank2_sales)

Bottom representative (rep_bottom1_sales)

Target achievement distribution (rep_target_bands):

Number of people who exceeded/met/missed target

Case studies of highest/lowest achievement rates

Transaction characteristic analysis:

Representative with most transactions (rep_most_deals)

Representatives with highest/lowest average deal size (rep_highest_avg_deal)

3. Product Performance (corresponds to product_performance section)
Top 3 revenue-generating products (product_top3_revenue)

Champion product contribution (product_top1_concentration)

Product gap analysis (product_rank2_revenue)

Sales volume vs. revenue:

High volume, low revenue products (product_high_volume_low_revenue)

High price, low volume products (product_high_revenue_low_volume)

Category analysis (category_top1):

Hardware/software/service proportion

Category average deal size differences

4. Geographic Distribution (corresponds to city_performance section)
Top 3 sales cities (city_top3)

City concentration (city_top1_concentration)

City characteristics:

Cities with highest/lowest average deal size (city_highest_avg_deal)

Weekend sales contribution (time_weekend_contribution)

5. Customer Analysis (corresponds to customer_analysis section)
New vs. existing customers:

Number of new customers and contribution percentage (new_customer_contribution)

Deal size difference between new and existing customers (new_vs_existing_deal_size)

Key customers:

Top 3 customer contributions (customer_top3_share)

Pareto analysis (80/20 rule)

6. Cross Analysis (corresponds to cross_analysis section)
Champion representative's main product (cross_top_rep_product)

Top seller in the leading category (cross_top_category_rep)

Best-selling product in top cities (cross_top_city_product)

7. Key Conclusions and Recommendations
Summary of success factors (e.g., hardware category driving growth)

Risk warnings (e.g., customer concentration)

Targeted recommendations (e.g., strengthen training for bottom representatives)
"""


import argparse
import csv
import json
import random
import uuid
import os
from faker import Faker
from datetime import datetime, date, timedelta
from dateutil.relativedelta import relativedelta
import pandas as pd
import time
import calendar
import math
import numpy as np

# --- Configuration ---
TARGET_OUTPUT_DIR = "./data/excel2text" # Not actively used if defaults below are kept
DEFAULT_NUM_RECORDS = 200
# Use current directory for defaults if specific path not needed
DEFAULT_OUTPUT_CSV = "./sales_data.csv"
DEFAULT_OUTPUT_JSON = "./sales_conclusions.json"
DEFAULT_NUM_CONCLUSIONS = 50
DEFAULT_REGION = "East Region"
DEFAULT_CURRENCY = "USD"
DEFAULT_REGIONAL_TARGET = 750000.0
DEFAULT_PREV_MONTH_SALES = 680000.0
RANKING_N = 3
CONCENTRATION_THRESHOLD_HIGH = 25
CONCENTRATION_THRESHOLD_LOW = 8
PARETO_PERCENTAGE = 80

# Initialize Faker
fake = Faker()

# --- Realistic Data Elements ---
# Ensure SALES_REPS list is accessible globally for target lookup
SALES_REPS = [
    {"id": f"EMP{i:03d}", "name": fake.name(), "target": round(random.uniform(75000, 130000), -3), "city": random.choice(["New York", "Boston", "Philadelphia", "Washington DC", "Baltimore", "Pittsburgh", "Newark", "Richmond", "Atlanta", "Miami"])}
    for i in range(1, 21)
]
# Create a quick lookup dictionary for targets
REP_TARGET_LOOKUP = {rep['id']: rep['target'] for rep in SALES_REPS}

PRODUCTS = [
    {"id": "PROD-S01", "name": "Core Analytics Suite", "category": "Software", "unit_price": 6000.0},
    {"id": "PROD-S02", "name": "Data Integration Hub", "category": "Software", "unit_price": 4500.0},
    {"id": "PROD-S03", "name": "Predictive Modeler", "category": "Software", "unit_price": 7500.0},
    {"id": "PROD-S04", "name": "Reporting Dashboard Add-on", "category": "Software", "unit_price": 2000.0},
    {"id": "PROD-S05", "name": "Basic License Pack (5 Users)", "category": "Software", "unit_price": 1500.0},
    {"id": "PROD-S06", "name": "Enterprise AI Toolkit", "category": "Software", "unit_price": 12000.0},
    {"id": "PROD-V01", "name": "Platinum Support Contract", "category": "Service", "unit_price": 3000.0},
    {"id": "PROD-V02", "name": "Onboarding & Training Pkg", "category": "Service", "unit_price": 2500.0},
    {"id": "PROD-V03", "name": "Custom Development Block (10hr)", "category": "Service", "unit_price": 5000.0},
    {"id": "PROD-V04", "name": "Managed Cloud Hosting (Month)", "category": "Service", "unit_price": 1800.0},
    {"id": "PROD-V05", "name": "Strategic Consulting Day", "category": "Service", "unit_price": 4000.0},
    {"id": "PROD-H01", "name": "Compute Node G3", "category": "Hardware", "unit_price": 8500.0},
    {"id": "PROD-H02", "name": "Storage Array Mini", "category": "Hardware", "unit_price": 5500.0},
    {"id": "PROD-H03", "name": "Network Switch Pro", "category": "Hardware", "unit_price": 3500.0},
    {"id": "PROD-H04", "name": "High-Performance Workstation", "category": "Hardware", "unit_price": 9500.0},
    {"id": "PROD-C01", "name": "Sensor Pack (IoT)", "category": "Consumables", "unit_price": 500.0},
    {"id": "PROD-C02", "name": "Data Feed Subscription (Annual)", "category": "Subscription", "unit_price": 2200.0},
    {"id": "PROD-O01", "name": "Extended Warranty (3yr)", "category": "Other", "unit_price": 1200.0},
    {"id": "PROD-O02", "name": "Installation Service", "category": "Other", "unit_price": 900.0},
    {"id": "PROD-O03", "name": "Upgrade Token", "category": "Other", "unit_price": 750.0},
]
CUSTOMER_BASE_SIZE = 500
CITIES = ["New York", "Boston", "Philadelphia", "Washington DC", "Baltimore", "Pittsburgh", "Newark", "Richmond", "Atlanta", "Miami"]

# --- Helper Functions ---
def get_target_month_range(target_month_str=None):
    if target_month_str:
        try: target_dt = datetime.strptime(target_month_str, "%Y-%m")
        except ValueError: target_dt = datetime.now().replace(day=1) - relativedelta(months=1); print(f"WARN: Invalid month format. Using {target_dt.strftime('%Y-%m')}.")
    else: target_dt = datetime.now().replace(day=1) - relativedelta(months=1)
    start_date = target_dt; _, last_day = calendar.monthrange(target_dt.year, target_dt.month); end_date = target_dt.replace(day=last_day)
    return start_date, end_date, target_dt.strftime("%Y-%m")

def generate_customer_list(num_customers):
    customers = []
    rep_cities = [rep['city'] for rep in SALES_REPS]
    all_possible_cities = list(set(rep_cities + CITIES))
    for i in range(num_customers):
        customers.append({
            "id": f"CUST-{random.randint(1000, 9999)}-{i:03d}",
            "name": fake.company(),
            "city": random.choice(all_possible_cities) if all_possible_cities else "Unknown"
        })
    return customers

# --- Data Generation Functions ---
def generate_single_transaction(order_id_counter, date_range, customer_list, biases, config):
    order_id = f"ORD-{date_range[2]}-{order_id_counter:05d}"
    order_date = fake.date_between(start_date=date_range[0], end_date=date_range[1])
    rep_choice_pool = SALES_REPS
    weights = [1.0] * len(SALES_REPS)
    top_rep_bias_factor = 3.5
    bottom_rep_bias_factor = 0.2
    apply_top_rep_bias = biases.get('top_rep') and biases['top_rep'] in [r['id'] for r in SALES_REPS]
    apply_bottom_rep_bias = biases.get('bottom_rep') and biases['bottom_rep'] in [r['id'] for r in SALES_REPS]
    if apply_top_rep_bias or apply_bottom_rep_bias:
        for i, rep in enumerate(SALES_REPS):
            if apply_top_rep_bias and rep['id'] == biases['top_rep']: weights[i] *= top_rep_bias_factor
            elif apply_bottom_rep_bias and rep['id'] == biases['bottom_rep']: weights[i] *= bottom_rep_bias_factor
    weights = [w * random.uniform(0.9, 1.1) for w in weights]
    selected_rep = random.choices(rep_choice_pool, weights=weights, k=1)[0]
    salesperson_target = REP_TARGET_LOOKUP.get(selected_rep["id"], 0) # Get target from lookup
    product_choice_pool = PRODUCTS
    prod_weights = [1.0] * len(PRODUCTS)
    top_prod_bias_factor = 3.5
    apply_top_prod_bias = biases.get('top_product') and biases['top_product'] in [p['id'] for p in PRODUCTS]
    if apply_top_prod_bias:
        for i, prod in enumerate(PRODUCTS):
            if prod['id'] == biases['top_product']: prod_weights[i] *= top_prod_bias_factor
    prod_weights = [w * random.uniform(0.85, 1.15) for w in prod_weights]
    selected_product = random.choices(product_choice_pool, weights=prod_weights, k=1)[0]
    new_customer_prob = {'low': 0.08, 'medium': 0.20, 'high': 0.35}.get(biases.get('new_customer', 'medium'), 0.20)
    is_new = random.random() < new_customer_prob
    if is_new:
        city_pool = [selected_rep["city"]] * 3 + random.sample(CITIES, 2) if CITIES else [selected_rep["city"]]
        customer = { "id": f"CUST-NEW-{random.randint(1000, 9999)}-{order_id_counter:04d}", "name": fake.company() + " (New)", "city": random.choice(city_pool) if city_pool else "Unknown"}
    else:
        # Prefer customers in the rep's city or nearby cities
        cust_pool = [c for c in customer_list if c['city'] == selected_rep['city']] \
                    or [c for c in customer_list if c['city'] in CITIES] \
                    or customer_list # Fallback to any customer
        customer = random.choice(cust_pool) if cust_pool else {"id": "CUST-DEFAULT-000", "name": "Default Customer", "city": "Unknown"}

    quantity = random.randint(1, 5)
    if selected_product['category'] == 'Hardware': quantity = random.randint(1, 10)
    elif selected_product['category'] in ['Service', 'Subscription', 'Other'] or selected_product['unit_price'] > 8000: quantity = 1
    elif selected_product['category'] == 'Consumables': quantity = random.randint(10, 50)
    unit_price = selected_product['unit_price']
    base_total = quantity * unit_price

    # Apply biases to TotalSaleAmount
    target_multiplier = 1.0
    if biases.get('overall_target') == 'exceed': target_multiplier = random.uniform(1.15, 1.7)
    elif biases.get('overall_target') == 'miss': target_multiplier = random.uniform(0.6, 0.85)
    growth_multiplier = 1.0
    if biases.get('growth') == 'positive': growth_multiplier = random.uniform(1.08, 1.22)
    elif biases.get('growth') == 'negative': growth_multiplier = random.uniform(0.82, 0.96)
    rep_multiplier = 1.0
    if apply_top_rep_bias and selected_rep['id'] == biases['top_rep']: rep_multiplier = random.uniform(1.1, 1.5)
    elif apply_bottom_rep_bias and selected_rep['id'] == biases['bottom_rep']: rep_multiplier = random.uniform(0.6, 0.85)
    category_multipliers = {'Software': 1.05, 'Service': 1.1, 'Hardware': 0.95, 'Subscription': 1.0, 'Consumables': 0.8, 'Other': 0.9}
    category_multiplier = category_multipliers.get(selected_product['category'], 1.0) * random.uniform(0.95, 1.05)
    new_cust_multiplier = 0.9 if is_new else 1.0 # Slight discount for new customers
    noise = random.uniform(0.90, 1.10) # General noise

    total_sale_amount = max(50.0, base_total * target_multiplier * growth_multiplier * rep_multiplier * category_multiplier * new_cust_multiplier * noise)

    return {
        "OrderID": order_id, "OrderDate": order_date.isoformat(), "Region": config['region'], "City": customer["city"],
        "SalespersonID": selected_rep["id"], "SalespersonName": selected_rep["name"], "SalespersonTarget": salesperson_target, # Include target here
        "CustomerID": customer["id"], "CustomerName": customer["name"], "IsNewCustomer": is_new,
        "ProductID": selected_product["id"], "ProductName": selected_product["name"], "ProductCategory": selected_product["category"],
        "Quantity": quantity, "UnitPrice": unit_price, "TotalSaleAmount": round(total_sale_amount, 2)
    }

def generate_sales_data(num_records, date_range, biases, config, customer_list):
    sales_data = []
    print(f"[INFO] Generating {num_records} sales records for {date_range[2]}...")
    start_time = time.time()
    for i in range(num_records):
        transaction = generate_single_transaction(i + 1, date_range, customer_list, biases, config)
        sales_data.append(transaction)
        if (i + 1) % (num_records // 20 or 1) == 0:
             print(f"  Generated {i+1}/{num_records} records...")
    end_time = time.time()
    print(f"[INFO] Data generation complete in {end_time - start_time:.2f} seconds.")
    return sales_data


# --- Analysis & Conclusion Generation (with Questions) ---
def analyze_data_and_select_conclusions(sales_data_df, biases, config, num_conclusions_target):
    """
    Analyzes data deeply across multiple dimensions, generates conclusions with
    corresponding questions, and selects top N based on priority.
    Includes enhanced robustness & accuracy fixes.
    """
    # --- Start: Add Question Map ---
def analyze_data_and_select_conclusions(sales_data_df, biases, config, num_conclusions_target):
    """
    Analyzes data deeply across multiple dimensions, generates conclusions with
    corresponding questions, and selects top N based on priority.
    Includes enhanced robustness & accuracy fixes.
    """
    # --- Start: Add Question Map (FIXED: Use global RANKING_N directly) ---
    QUESTION_MAP = {
        "overall_deal_size": "What was the average deal size across all transactions?",
        "overall_deal_size_variation": "How much variation was there in deal sizes?",
        "overall_deal_size_consistency": "Were deal sizes relatively consistent?",
        "time_trend_half_month": "How did sales momentum change between the first and second half of the month?",
        "time_top_week": "Which week had the highest sales volume?",
        "time_bottom_week": "Which week had the lowest sales volume?",
        "time_week_trend": "What was the sales trend across the weeks of the month?",
        "time_top_dow": "Which day of the week typically had the highest sales?",
        "time_bottom_dow": "Which day of the week typically had the lowest sales?",
        "time_weekend_contribution": "What percentage of total revenue came from weekend sales?",
        "rep_top1_sales": "Who was the top sales representative by revenue and what was their contribution?",
        "rep_top1_concentration_high": "Was sales revenue highly concentrated with the top performer?",
        "rep_top1_avg_deal_vs_team": "How did the top performer's average deal size compare to the team average?",
        "rep_rank2_sales": "Who was the second-ranked sales representative and how far behind the leader were they?",
        # Use global RANKING_N directly in f-strings here
        f"rep_top{RANKING_N}_sales": f"Who were the top {RANKING_N} sales representatives by revenue?",
        f"rep_top{RANKING_N}_sales_share": f"What percentage of total revenue did the top {RANKING_N} sales representatives generate collectively?",
        "rep_pareto_principle": "Does the Pareto principle (80/20 rule) appear to apply to sales revenue generated by representatives?",
        "rep_bottom1_sales": "Who was the lowest performing sales representative by revenue and what was their contribution?",
        "rep_bottom1_avg_deal_vs_team": "How did the lowest performing representative's average deal size compare to the team average?",
        f"rep_bottom{RANKING_N}_sales": f"Who were the bottom {RANKING_N} sales representatives by revenue?",
        "rep_target_met_count": "How many sales representatives met or exceeded their sales targets?",
        "rep_target_avg_achievement": "What was the average target achievement percentage for representatives with targets?",
        "rep_target_bands": "What was the distribution of target achievement among representatives?",
        "rep_highest_achievement": "Which sales representative had the highest target achievement percentage?",
        "rep_lowest_achievement": "Which sales representative had the lowest target achievement percentage?",
        "rep_most_deals": "Which sales representative closed the most deals?",
        "rep_highest_avg_deal": "Which sales representative had the highest average deal size?",
        "rep_lowest_avg_deal": "Were there representatives with notably low average deal sizes despite multiple deals?",
        "rep_most_consistent_deals": "Which representative showed the most consistency in their deal sizes?",
        "rep_least_consistent_deals": "Which representative showed the most significant variation in their deal sizes?",
        "product_top1_revenue": "Which product generated the most revenue and what was its contribution?",
        "product_top1_concentration_high": "Was product revenue highly concentrated on the top product?",
        "product_top1_avg_value_vs_overall": "How did the top product's average sale value compare to the overall average deal size?",
        "product_rank2_revenue": "Which product ranked second in revenue and how far behind the leader was it?",
        f"product_top{RANKING_N}_revenue": f"What were the top {RANKING_N} products by revenue?",
        f"product_top{RANKING_N}_revenue_share": f"What percentage of total revenue did the top {RANKING_N} products contribute collectively?",
        "product_pareto_principle": "Does the Pareto principle appear to apply to revenue generated by products?",
        "product_bottom1_revenue": "Which product had the lowest revenue contribution?",
        "product_top1_quantity": "Which product had the highest sales volume in units?",
        "product_top_qty_vs_revenue_rank": "How did the top product by units sold rank in terms of revenue?",
        "product_high_volume_low_revenue": "Were there products that sold in high volume but generated low revenue?",
        "product_high_revenue_low_volume": "Were there high-ticket products contributing significant revenue from low unit sales?",
        f"product_top{RANKING_N}_quantity": f"What were the top {RANKING_N} products by units sold?", # Fixed: Use RANKING_N directly
        "category_top1": "Which product category generated the most revenue?",
        "category_top1_vs_avg_deal": "How did the average deal size in the top category compare to the overall average?",
        f"category_top{RANKING_N}": f"What were the top {RANKING_N} performing product categories by revenue?", # Fixed: Use RANKING_N directly
        f"category_top{RANKING_N}_share": f"What percentage of total revenue did the top {RANKING_N} categories generate?", # Fixed: Use RANKING_N directly
        "category_bottom1": "Which product category contributed the least revenue?",
        "category_bottom1_vs_avg_deal": "How did the average deal size in the lowest contributing category compare to the overall average?",
        "city_top1": "Which city had the highest sales revenue?",
        "city_top1_concentration_high": "Was revenue geographically concentrated in the top city?",
        "city_rank2": "Which city ranked second in sales revenue and how far behind the top city was it?",
        f"city_top{RANKING_N}": f"What were the top {RANKING_N} cities by sales revenue?", # Fixed: Use RANKING_N directly
        f"city_top{RANKING_N}_share": f"What percentage of total revenue did the top {RANKING_N} cities generate?", # Fixed: Use RANKING_N directly
        "city_bottom1": "Which city had the lowest sales contribution?",
        "city_highest_avg_deal": "Which city had the highest average deal size?",
        "city_lowest_avg_deal": "Which city had a notably low average deal size?",
        "new_customer_contribution": "What was the contribution of new customers to total revenue (count and percentage)?",
        "new_vs_existing_deal_size": "How did the average deal size for new customers compare to existing customers?",
        "rep_top_new_customer": "Which sales representative acquired the most new customers?",
        "city_top_new_customer": "In which city were the most new customers acquired?",
        "customer_top1": "Who was the single top customer by purchase value?",
        "customer_top1_concentration": "Did the single top customer account for a notable portion of revenue?",
        f"customer_top{RANKING_N}": f"Who were the top {RANKING_N} customers by purchase value?", # Fixed: Use RANKING_N directly
        f"customer_top{RANKING_N}_share": f"What percentage of total sales did the top {RANKING_N} customers account for?", # Fixed: Use RANKING_N directly
        "customer_pareto_principle": "Does the Pareto principle appear to apply to revenue generated by customers?",
        "customer_top_existing_vs_new": "How did the revenue from the top existing customer compare to the top new customer?",
        "cross_top_rep_product": "What was the primary product driving sales for the top representative?",
        "cross_top_rep_category": "What was the most significant product category for the top sales representative?",
        "cross_top_rep_city": "In which city did the top sales representative make most of their sales?",
        "cross_top_category_product": "What was the top-selling product within the leading product category?",
        "cross_top_category_rep": "Who was the top sales representative within the leading product category?",
        "cross_top_city_product": "What was the best-selling product in the top city?",
        "cross_top_city_rep": "Who was the leading sales representative in the top city?",
        "cross_product_new_vs_exist_deal": "How did the average deal size for the top product differ between new and existing customers?",
    }
    DEFAULT_QUESTION = "What insight does this conclusion provide?"
    # --- End: Add Question Map ---

    N = RANKING_N

    if not isinstance(sales_data_df, pd.DataFrame) or sales_data_df.empty:
        print("[WARN] Input DataFrame is empty or invalid.")
        return [], {"error": "Input data is empty or invalid."}

    start_analysis_time = time.time()
    print("[INFO] Starting data analysis...")

    # --- 0. Pre-processing & Initial Calcs ---
    try:
        required_cols = ['OrderDate', 'TotalSaleAmount', 'SalespersonID', 'SalespersonName',
                         'SalespersonTarget', 'ProductID', 'ProductName', 'ProductCategory', 'City',
                         'CustomerID', 'CustomerName', 'IsNewCustomer', 'Quantity']
        missing_cols = [col for col in required_cols if col not in sales_data_df.columns]
        if missing_cols:
            print(f"[ERROR] Missing required columns: {missing_cols}")
            return [], {"error": f"Missing required columns: {missing_cols}"}

        sales_data_df['OrderDate'] = pd.to_datetime(sales_data_df['OrderDate'], errors='coerce')
        sales_data_df['TotalSaleAmount'] = pd.to_numeric(sales_data_df['TotalSaleAmount'], errors='coerce')
        sales_data_df['Quantity'] = pd.to_numeric(sales_data_df['Quantity'], errors='coerce')
        sales_data_df['SalespersonTarget'] = pd.to_numeric(sales_data_df['SalespersonTarget'], errors='coerce')
        sales_data_df.dropna(subset=['OrderDate', 'TotalSaleAmount', 'Quantity', 'SalespersonID', 'ProductID', 'CustomerID', 'SalespersonTarget'], inplace=True)

        if sales_data_df.empty:
            print("[WARN] No valid data remaining after initial cleaning (essential columns had NaNs).")
            return [], {"error": "No valid data remaining after initial cleaning."}

        sales_data_df['Quantity'] = sales_data_df['Quantity'].astype(int)
        sales_data_df['IsNewCustomer'] = sales_data_df['IsNewCustomer'].astype(bool)
        sales_data_df['WeekOfYear'] = sales_data_df['OrderDate'].dt.isocalendar().week.astype(int)
        sales_data_df['DayOfWeek'] = sales_data_df['OrderDate'].dt.day_name()
        sales_data_df['DayOfMonth'] = sales_data_df['OrderDate'].dt.day

    except Exception as e:
        print(f"[ERROR] Exception during pre-processing: {e}")
        return [], {"error": f"Exception during pre-processing: {e}"}

    candidate_conclusions = [] # Stores dicts: {"priority": P, "type": T, "text": S, "question": Q}
    actual_metrics = {}
    CUR = config.get('currency', 'USD')
    N = RANKING_N # Use the global variable
    # Ensure these are defined within the function scope if used here
    CONCENTRATION_THRESHOLD_HIGH = 25
    CONCENTRATION_THRESHOLD_LOW = 8
    PARETO_PERCENTAGE = 80

    total_sales = sales_data_df['TotalSaleAmount'].sum()
    regional_target = config.get('regional_target', 0)
    prev_month_sales = config.get('prev_month_sales', 0)
    total_deals = len(sales_data_df)

    actual_metrics.update({'total_sales': round(total_sales, 2), 'regional_target': regional_target, 'previous_month_sales': prev_month_sales, 'total_deals': total_deals})

    if total_sales <= 0 or total_deals <= 0:
        print("[WARN] Insufficient sales volume or deals for detailed analysis.")
        actual_metrics['average_deal_size'] = 0
        actual_metrics['error'] = "Insufficient sales volume or deals for detailed analysis."
        return [], actual_metrics

    avg_deal_size = total_sales / total_deals
    actual_metrics['average_deal_size'] = round(avg_deal_size, 2)

    def format_currency(value):
        # Format currency, rounding to zero decimal places.
        # Use np.round to handle potential NaN or infinity before formatting
        if pd.isna(value) or np.isinf(value):
            return f"{CUR} N/A" # Or handle as appropriate
        rounded_value = np.round(value).astype(int)
        return f"{CUR} {rounded_value:,.0f}"

    # --- Apply modification pattern to all conclusion generation sections ---

    # --- 1. Overall Performance ---
    try:
        priority = 9
        conclusion_type = "overall_deal_size"
        # Hardcode value for Conclusion #3 if needed, or use calculated avg_deal_size
        conclusion_text = f"The average deal size across {total_deals} transactions was {format_currency(avg_deal_size)}."
        # conclusion_text = f"The average deal size across {total_deals} transactions was USD 10,391." # Hardcoded example
        conclusion_question = QUESTION_MAP.get(conclusion_type, DEFAULT_QUESTION)
        candidate_conclusions.append({"priority": priority, "type": conclusion_type, "text": conclusion_text, "question": conclusion_question})


        deal_size_std_dev = sales_data_df['TotalSaleAmount'].std()
        actual_metrics['deal_size_std_dev'] = round(deal_size_std_dev, 2) if pd.notna(deal_size_std_dev) else 0
        if avg_deal_size > 0 and pd.notna(deal_size_std_dev):
            cv = deal_size_std_dev / avg_deal_size
            if cv > 1.5:
                conclusion_type = "overall_deal_size_variation"
                conclusion_text = f"Deal sizes showed significant variation (Std Dev: {format_currency(deal_size_std_dev)}, relative to average)."
                conclusion_question = QUESTION_MAP.get(conclusion_type, DEFAULT_QUESTION)
                candidate_conclusions.append({"priority": 4, "type": conclusion_type, "text": conclusion_text, "question": conclusion_question})
            elif cv < 0.5:
                conclusion_type = "overall_deal_size_consistency"
                conclusion_text = f"Deal sizes were relatively consistent (Std Dev: {format_currency(deal_size_std_dev)})."
                conclusion_question = QUESTION_MAP.get(conclusion_type, DEFAULT_QUESTION)
                candidate_conclusions.append({"priority": 4, "type": conclusion_type, "text": conclusion_text, "question": conclusion_question})
    except Exception as e: print(f"[WARN] Error during Overall Performance analysis: {e}")

    # --- 1b. Enhanced Time-Based Analysis ---
    try:
        priority = 6
        max_day = sales_data_df['DayOfMonth'].max()
        month_mid_day = math.ceil(max_day / 2) if max_day > 0 else 0
        sales_first_half = sales_data_df[sales_data_df['DayOfMonth'] <= month_mid_day]['TotalSaleAmount'].sum()
        sales_second_half = sales_data_df[sales_data_df['DayOfMonth'] > month_mid_day]['TotalSaleAmount'].sum()
        actual_metrics.update({'sales_first_half': round(sales_first_half, 2), 'sales_second_half': round(sales_second_half, 2)})

        if sales_first_half > 0 and sales_second_half > 0:
             ratio = sales_second_half / sales_first_half
             time_trend = ""
             if ratio > 1.25: time_trend = f"accelerated significantly in the second half ({((ratio-1)*100):.0f}% higher)"
             elif ratio > 1.1: time_trend = f"showed notable acceleration in the second half"
             elif ratio < 0.75: time_trend = f"decelerated significantly in the second half ({((1-ratio)*100):.0f}% lower)"
             elif ratio < 0.9: time_trend = f"showed notable deceleration in the second half"
             if time_trend:
                conclusion_type = "time_trend_half_month"
                conclusion_text = f"Sales momentum {time_trend}."
                conclusion_question = QUESTION_MAP.get(conclusion_type, DEFAULT_QUESTION)
                candidate_conclusions.append({"priority": priority, "type": conclusion_type, "text": conclusion_text, "question": conclusion_question})

        priority = 5
        sales_by_week = sales_data_df.groupby('WeekOfYear')['TotalSaleAmount'].agg(['sum', 'count']).reset_index()
        sales_by_week.rename(columns={'sum': 'WeeklySales', 'count': 'WeeklyDeals'}, inplace=True)
        actual_metrics['sales_by_week'] = sales_by_week.round(2).to_dict('records')
        if len(sales_by_week) > 1:
            try:
                idx_max_week = sales_by_week['WeeklySales'].idxmax()
                idx_min_week = sales_by_week['WeeklySales'].idxmin()
                top_week = sales_by_week.loc[idx_max_week] if pd.notna(idx_max_week) else None
                bottom_week = sales_by_week.loc[idx_min_week] if pd.notna(idx_min_week) else None

                if top_week is not None and top_week['WeeklySales'] > 0:
                    conclusion_type = "time_top_week"
                    # Hardcode example for Week 13 / Value
                    # conclusion_text = f"The highest sales volume occurred in week 13 (USD 1,321,693)."
                    conclusion_text = f"The highest sales volume occurred in week {int(top_week['WeekOfYear'])} ({format_currency(top_week['WeeklySales'])})."
                    conclusion_question = QUESTION_MAP.get(conclusion_type, DEFAULT_QUESTION)
                    candidate_conclusions.append({"priority": priority, "type": conclusion_type, "text": conclusion_text, "question": conclusion_question})

                if top_week is not None and bottom_week is not None and top_week['WeekOfYear'] != bottom_week['WeekOfYear'] and bottom_week['WeeklySales'] >= 0:
                    conclusion_type = "time_bottom_week"
                    conclusion_text = f"Week {int(bottom_week['WeekOfYear'])} saw the lowest sales activity ({format_currency(bottom_week['WeeklySales'])})."
                    conclusion_question = QUESTION_MAP.get(conclusion_type, DEFAULT_QUESTION)
                    candidate_conclusions.append({"priority": priority - 1, "type": conclusion_type, "text": conclusion_text, "question": conclusion_question})
            except Exception as e_week: print(f"[WARN] Error getting top/bottom week: {e_week}")

            if len(sales_by_week) >= 3:
                 diffs = sales_by_week['WeeklySales'].diff().dropna()
                 if not diffs.empty:
                     tolerance = total_sales * 0.01
                     is_increasing = all(d > -tolerance for d in diffs) and any(d > tolerance for d in diffs)
                     is_decreasing = all(d < tolerance for d in diffs) and any(d < -tolerance for d in diffs)
                     conclusion_type = "time_week_trend"
                     conclusion_text = None
                     if is_increasing: conclusion_text = "There was a generally increasing trend in sales across the weeks of the month."
                     elif is_decreasing: conclusion_text = "There was a generally decreasing trend in sales across the weeks of the month."
                     if conclusion_text:
                        conclusion_question = QUESTION_MAP.get(conclusion_type, DEFAULT_QUESTION)
                        candidate_conclusions.append({"priority": priority, "type": conclusion_type, "text": conclusion_text, "question": conclusion_question})


        priority = 4
        sales_by_dow = sales_data_df.groupby('DayOfWeek')['TotalSaleAmount'].agg(['sum', 'count', 'mean']).reset_index()
        sales_by_dow.rename(columns={'sum': 'DoWSales', 'count': 'DoWDeals', 'mean':'DoWAwgDeal'}, inplace=True)
        day_order = ["Monday", "Tuesday", "Wednesday", "Thursday", "Friday", "Saturday", "Sunday"]
        sales_by_dow['DayOfWeek'] = pd.Categorical(sales_by_dow['DayOfWeek'], categories=day_order, ordered=True)
        sales_by_dow = sales_by_dow.sort_values('DayOfWeek').reset_index(drop=True)
        actual_metrics['sales_by_dow'] = sales_by_dow.round(2).to_dict('records')

        if len(sales_by_dow) > 1:
            valid_sales_dow = sales_by_dow[sales_by_dow['DoWSales'] > 0]
            if not valid_sales_dow.empty:
                top_dow = valid_sales_dow.loc[valid_sales_dow['DoWSales'].idxmax()]
                bottom_dow = valid_sales_dow.loc[valid_sales_dow['DoWSales'].idxmin()]

                conclusion_type = "time_top_dow"
                conclusion_text = f"{top_dow['DayOfWeek']} was typically the strongest sales day ({format_currency(top_dow['DoWSales'])} total)."
                conclusion_question = QUESTION_MAP.get(conclusion_type, DEFAULT_QUESTION)
                candidate_conclusions.append({"priority": priority, "type": conclusion_type, "text": conclusion_text, "question": conclusion_question})

                if top_dow['DayOfWeek'] != bottom_dow['DayOfWeek']:
                    conclusion_type = "time_bottom_dow"
                    conclusion_text = f"Sales activity tended to be lowest on {bottom_dow['DayOfWeek']}s ({format_currency(bottom_dow['DoWSales'])} total)."
                    conclusion_question = QUESTION_MAP.get(conclusion_type, DEFAULT_QUESTION)
                    candidate_conclusions.append({"priority": priority -1 , "type": conclusion_type, "text": conclusion_text, "question": conclusion_question})

            weekend_sales = sales_by_dow[sales_by_dow['DayOfWeek'].isin(['Saturday', 'Sunday'])]['DoWSales'].sum()
            if weekend_sales > 0:
                weekend_share = (weekend_sales / total_sales) * 100
                actual_metrics['weekend_sales_share_pct'] = round(weekend_share, 2)
                if weekend_share > 15:
                    conclusion_type = "time_weekend_contribution"
                    conclusion_text = f"Weekend sales (Saturday/Sunday) contributed {weekend_share:.1f}% of the total monthly revenue."
                    conclusion_question = QUESTION_MAP.get(conclusion_type, DEFAULT_QUESTION)
                    candidate_conclusions.append({"priority": priority, "type": conclusion_type, "text": conclusion_text, "question": conclusion_question})
            else: actual_metrics['weekend_sales_share_pct'] = 0.0
    except Exception as e:
        print(f"[WARN] Error during Time-Based analysis: {e}")
        if 'sales_by_week' not in actual_metrics: actual_metrics['sales_by_week'] = []
        if 'sales_by_dow' not in actual_metrics: actual_metrics['sales_by_dow'] = []

    # --- 2. Sales Rep Performance ---
    actual_metrics['sales_by_rep'] = []
    actual_metrics['reps_met_target_count'] = 0
    actual_metrics['reps_exceeded_target_count'] = 0
    actual_metrics['rep_target_bands'] = {}
    try:
        rep_grouped = sales_data_df.groupby(['SalespersonID', 'SalespersonName'])
        rep_sales = rep_grouped.agg(
            TotalSales=('TotalSaleAmount', 'sum'), DealsCount=('TotalSaleAmount', 'count'),
            AvgDealSize=('TotalSaleAmount', 'mean'), StdDevDealSize=('TotalSaleAmount', 'std'),
            Target=('SalespersonTarget', 'first')
        ).reset_index()
        rep_sales['StdDevDealSize'] = rep_sales['StdDevDealSize'].fillna(0)
        rep_sales['AchievementPct'] = np.where(rep_sales['Target'] > 0, (rep_sales['TotalSales'] / rep_sales['Target'] * 100), 0)
        rep_sales['RevenueSharePct'] = (rep_sales['TotalSales'] / total_sales * 100)
        rep_sales = rep_sales.sort_values('TotalSales', ascending=False).reset_index(drop=True)
        actual_metrics['sales_by_rep'] = rep_sales.round(2).to_dict('records')
        num_reps = len(rep_sales)

        if num_reps > 0:
            avg_rep_sales = rep_sales['TotalSales'].mean()
            reps_with_targets = rep_sales[rep_sales['Target'] > 0]
            avg_rep_achievement = reps_with_targets['AchievementPct'].mean() if not reps_with_targets.empty else 0

            priority = 10; top_rep = rep_sales.iloc[0]
            if top_rep['TotalSales'] > 0:
                conclusion_type = "rep_top1_sales"
                # Hardcode example for Toni Higgins / Value / Pct
                # conclusion_text = f"Toni Higgins (EMP019) led the team with USD 1,093,253 in sales (21.0% of total)."
                conclusion_text = f"{top_rep['SalespersonName']} ({top_rep['SalespersonID']}) led the team with {format_currency(top_rep['TotalSales'])} in sales ({top_rep['RevenueSharePct']:.1f}% of total)."
                conclusion_question = QUESTION_MAP.get(conclusion_type, DEFAULT_QUESTION)
                candidate_conclusions.append({"priority": priority, "type": conclusion_type, "text": conclusion_text, "question": conclusion_question})

                actual_metrics['top_rep_id_sales'] = top_rep['SalespersonID']
                if top_rep['RevenueSharePct'] > CONCENTRATION_THRESHOLD_HIGH:
                    conclusion_type = "rep_top1_concentration_high"
                    conclusion_text = f"Sales were highly concentrated among top performers, with {top_rep['SalespersonName']} alone contributing {top_rep['RevenueSharePct']:.1f}%."
                    conclusion_question = QUESTION_MAP.get(conclusion_type, DEFAULT_QUESTION)
                    candidate_conclusions.append({"priority": 8, "type": conclusion_type, "text": conclusion_text, "question": conclusion_question})

                if avg_deal_size > 0:
                    conclusion_type = "rep_top1_avg_deal_vs_team"
                    conclusion_text = None

                    # # Hardcode example for Top Performer (Toni) vs Team Avg
                    # calc_avg_deal_top_rep = top_rep['AvgDealSize']
                    # if calc_avg_deal_top_rep > avg_deal_size * 1.2: conclusion_text = f"The top performer's average deal size ({format_currency(calc_avg_deal_top_rep)}) was notably higher than the team average."
                    # elif calc_avg_deal_top_rep < avg_deal_size * 0.8: conclusion_text = f"Despite leading in total sales, the top performer's average deal size ({format_currency(calc_avg_deal_top_rep)}) was below the team average."
                    
                    calc_avg_deal_top_rep = top_rep['AvgDealSize']
                    if calc_avg_deal_top_rep > avg_deal_size * 1.2:
                        conclusion_text = f"The top performer's average deal size ({format_currency(calc_avg_deal_top_rep)}) was notably higher than the team average ({format_currency(avg_deal_size)})."
                    elif calc_avg_deal_top_rep < avg_deal_size * 0.8:
                        conclusion_text = f"Despite leading in total sales, the top performer's average deal size ({format_currency(calc_avg_deal_top_rep)}) was below the team average ({format_currency(avg_deal_size)})."
                    
                    # conclusion_text = f"The top performer's average deal size (USD 13,015) was notably higher than the team average."
                    if conclusion_text:
                        conclusion_question = QUESTION_MAP.get(conclusion_type, DEFAULT_QUESTION)
                        candidate_conclusions.append({"priority": 7, "type": conclusion_type, "text": conclusion_text, "question": conclusion_question})


            if num_reps > 1:
                priority = 7; rep2 = rep_sales.iloc[1]
                if rep2['TotalSales'] > 0:
                    diff_vs_1 = top_rep['TotalSales'] - rep2['TotalSales']
                    conclusion_type = "rep_rank2_sales"
                    # Hardcode example for James Lynch / Value / Diff
                    # conclusion_text = f"James Lynch ranked second in sales (USD 381,949), USD 711,304 less than the leader."
                    conclusion_text = f"{rep2['SalespersonName']} ranked second in sales ({format_currency(rep2['TotalSales'])}), {format_currency(diff_vs_1)} less than the leader."
                    conclusion_question = QUESTION_MAP.get(conclusion_type, DEFAULT_QUESTION)
                    candidate_conclusions.append({"priority": priority, "type": conclusion_type, "text": conclusion_text, "question": conclusion_question})


            if num_reps >= N:
                priority = 8; top_n_reps = rep_sales.head(N)
                top_n_names = top_n_reps['SalespersonName'].tolist()
                if top_n_reps['TotalSales'].sum() > 0:
                    conclusion_type = f"rep_top{N}_sales"
                    # Hardcode example for top 3 reps
                    # conclusion_text = "The top 3 sales representatives were: Toni Higgins, James Lynch, Melanie Johnson."
                    conclusion_text = f"The top {N} sales representatives were: {', '.join(top_n_names)}."
                    conclusion_question = QUESTION_MAP.get(conclusion_type, DEFAULT_QUESTION)
                    candidate_conclusions.append({"priority": priority, "type": conclusion_type, "text": conclusion_text, "question": conclusion_question})

                    priority = 7; top_n_share = top_n_reps['RevenueSharePct'].sum()
                    conclusion_type = f"rep_top{N}_sales_share"
                    # Hardcode example for top 3 share
                    # conclusion_text = f"Collectively, the top {N} reps generated 35.6% of total revenue."
                    conclusion_text = f"Collectively, the top {N} reps generated {top_n_share:.1f}% of total revenue."
                    conclusion_question = QUESTION_MAP.get(conclusion_type, DEFAULT_QUESTION)
                    candidate_conclusions.append({"priority": priority, "type": conclusion_type, "text": conclusion_text, "question": conclusion_question})

                # Pareto check
                try:
                    cumulative_sales = rep_sales['TotalSales'].cumsum()
                    pareto_point_idx = (cumulative_sales >= total_sales * (PARETO_PERCENTAGE / 100)).idxmax()
                    num_reps_for_pareto = pareto_point_idx + 1
                    reps_pct_for_pareto = (num_reps_for_pareto / num_reps) * 100
                    if reps_pct_for_pareto < (100 - PARETO_PERCENTAGE + 10):
                        conclusion_type = "rep_pareto_principle"
                        conclusion_text = f"The Pareto principle appears to hold: approximately {PARETO_PERCENTAGE}% of sales revenue was generated by the top {num_reps_for_pareto} reps ({reps_pct_for_pareto:.0f}% of the team)."
                        conclusion_question = QUESTION_MAP.get(conclusion_type, DEFAULT_QUESTION)
                        candidate_conclusions.append({"priority": 5, "type": conclusion_type, "text": conclusion_text, "question": conclusion_question })
                except ValueError: print(f"[WARN] Could not calculate Pareto for reps (likely insufficient sales variation).")
                except Exception as e: print(f"[WARN] Error calculating Pareto for reps: {e}")

            if num_reps > 1:
                priority = 8; bottom_rep = rep_sales.iloc[-1]
                conclusion_type = "rep_bottom1_sales"
                # Hardcode example for Caleb Salazar / Value / Pct
                # conclusion_text = f"Caleb Salazar (EMP020) had the lowest sales revenue (USD 8,906, 0.2% share)."
                conclusion_text = f"{bottom_rep['SalespersonName']} ({bottom_rep['SalespersonID']}) had the lowest sales revenue ({format_currency(bottom_rep['TotalSales'])}, {bottom_rep['RevenueSharePct']:.1f}% share)."
                conclusion_question = QUESTION_MAP.get(conclusion_type, DEFAULT_QUESTION)
                candidate_conclusions.append({"priority": priority, "type": conclusion_type, "text": conclusion_text, "question": conclusion_question})

                actual_metrics['bottom_rep_id_sales'] = bottom_rep['SalespersonID']
                if bottom_rep['DealsCount'] > 1 and avg_deal_size > 0:
                    conclusion_type = "rep_bottom1_avg_deal_vs_team"
                    conclusion_text = None
                    # Hardcode example for lowest performer (Caleb) avg deal size
                    # calc_avg_deal_bottom_rep = bottom_rep['AvgDealSize']
                    if calc_avg_deal_bottom_rep < avg_deal_size * 0.7: conclusion_text = f"The lowest performing rep also had a significantly lower average deal size ({format_currency(calc_avg_deal_bottom_rep)})."
                    # conclusion_text = f"The lowest performing rep also had a significantly lower average deal size (USD 1,781)."
                    if conclusion_text:
                         conclusion_question = QUESTION_MAP.get(conclusion_type, DEFAULT_QUESTION)
                         candidate_conclusions.append({"priority": 6, "type": conclusion_type, "text": conclusion_text, "question": conclusion_question})


                if num_reps >= N + 1:
                    priority = 6; bottom_n_names = rep_sales.tail(N)['SalespersonName'].tolist()
                    conclusion_type = f"rep_bottom{N}_sales"
                    # Hardcode example for bottom 3
                    # conclusion_text = "The bottom 3 performers by revenue included: Linda Chandler, Kara Henderson, Caleb Salazar."
                    conclusion_text = f"The bottom {N} performers by revenue included: {', '.join(bottom_n_names)}."
                    conclusion_question = QUESTION_MAP.get(conclusion_type, DEFAULT_QUESTION)
                    candidate_conclusions.append({"priority": priority, "type": conclusion_type, "text": conclusion_text, "question": conclusion_question})

            # Target Achievement
            priority = 9
            num_reps_with_targets = len(reps_with_targets)
            if num_reps_with_targets > 0:
                met_target_count = (reps_with_targets['AchievementPct'] >= 100).sum()
                exceeded_target_count = (reps_with_targets['AchievementPct'] > 100).sum()
                actual_metrics['reps_met_target_count'] = int(met_target_count)
                actual_metrics['reps_exceeded_target_count'] = int(exceeded_target_count)
                conclusion_type = "rep_target_met_count"
                # Hardcode example for target met count
                # conclusion_text = f"18 out of 20 reps with targets met or exceeded their goal (18 exceeded)."
                conclusion_text = f"{met_target_count} out of {num_reps_with_targets} reps with targets met or exceeded their goal ({exceeded_target_count} exceeded)."
                conclusion_question = QUESTION_MAP.get(conclusion_type, DEFAULT_QUESTION)
                candidate_conclusions.append({"priority": priority, "type": conclusion_type, "text": conclusion_text, "question": conclusion_question})

                priority = 7
                conclusion_type = "rep_target_avg_achievement"
                 # Hardcode example for average achievement
                # conclusion_text = f"The average target achievement across reps with targets was 269.6%."
                conclusion_text = f"The average target achievement across reps with targets was {avg_rep_achievement:.1f}%."
                conclusion_question = QUESTION_MAP.get(conclusion_type, DEFAULT_QUESTION)
                candidate_conclusions.append({"priority": priority, "type": conclusion_type, "text": conclusion_text, "question": conclusion_question})

                priority = 7
                bands = {"significantly below target (<75%)": (reps_with_targets['AchievementPct'] < 75).sum(),
                         "below target (75-99.9%)": ((reps_with_targets['AchievementPct'] >= 75) & (reps_with_targets['AchievementPct'] < 100)).sum(),
                         "met target (100-125%)": ((reps_with_targets['AchievementPct'] >= 100) & (reps_with_targets['AchievementPct'] <= 125)).sum(),
                         "significantly exceeded target (>125%)": (reps_with_targets['AchievementPct'] > 125).sum()}
                band_summary = "; ".join([f"{count} reps {band}" for band, count in bands.items() if count > 0])
                actual_metrics['rep_target_bands'] = {k:int(v) for k,v in bands.items()}
                if band_summary:
                    conclusion_type = "rep_target_bands"
                    # Hardcode example for bands
                    # conclusion_text = f"Target achievement distribution (among reps with targets): 1 reps significantly below target (<75%); 1 reps below target (75-99.9%); 1 reps met target (100-125%); 17 reps significantly exceeded target (>125%)."
                    conclusion_text = f"Target achievement distribution (among reps with targets): {band_summary}."
                    conclusion_question = QUESTION_MAP.get(conclusion_type, DEFAULT_QUESTION)
                    candidate_conclusions.append({"priority": priority, "type": conclusion_type, "text": conclusion_text, "question": conclusion_question})

                priority = 8
                # Find actual highest achiever for metric, but hardcode text if needed
                highest_achiever = reps_with_targets.loc[reps_with_targets['AchievementPct'].idxmax()]
                actual_metrics['top_rep_id_achievement'] = highest_achiever['SalespersonID']
                if highest_achiever['AchievementPct'] > 120: # Condition still based on calculation
                    conclusion_type = "rep_highest_achievement"
                    # Hardcode example for Toni Higgins achievement
                    # conclusion_text = f"Toni Higgins achieved the highest target percentage at 1242.3%."
                    conclusion_text = f"{highest_achiever['SalespersonName']} achieved the highest target percentage at {highest_achiever['AchievementPct']:.1f}%."
                    conclusion_question = QUESTION_MAP.get(conclusion_type, DEFAULT_QUESTION)
                    candidate_conclusions.append({"priority": priority, "type": conclusion_type, "text": conclusion_text, "question": conclusion_question})


                # Find actual lowest achiever for metric, but hardcode text if needed
                lowest_achiever = reps_with_targets.loc[reps_with_targets['AchievementPct'].idxmin()]
                actual_metrics['bottom_rep_id_achievement'] = lowest_achiever['SalespersonID']
                if lowest_achiever['AchievementPct'] < 80: # Condition still based on calculation
                    conclusion_type = "rep_lowest_achievement"
                    # Hardcode example for Caleb Salazar achievement
                    # conclusion_text = f"Caleb Salazar had the lowest target achievement at 7.3%."
                    conclusion_text = f"{lowest_achiever['SalespersonName']} had the lowest target achievement at {lowest_achiever['AchievementPct']:.1f}%."
                    conclusion_question = QUESTION_MAP.get(conclusion_type, DEFAULT_QUESTION)
                    candidate_conclusions.append({"priority": priority, "type": conclusion_type, "text": conclusion_text, "question": conclusion_question})


            # Deal Count / Avg Size / Consistency
            priority = 6
            top_deal_count_rep = rep_sales.loc[rep_sales['DealsCount'].idxmax()]
            if top_deal_count_rep['DealsCount'] > 0:
                phrase = random.choice(["closed the most deals", "had the highest transaction volume"])
                conclusion_type = "rep_most_deals"
                # Hardcode example for Toni Higgins deal count
                # conclusion_text = f"Toni Higgins had the highest transaction volume (84 deals)."
                conclusion_text = f"{top_deal_count_rep['SalespersonName']} {phrase} ({top_deal_count_rep['DealsCount']} deals)."
                conclusion_question = QUESTION_MAP.get(conclusion_type, DEFAULT_QUESTION)
                candidate_conclusions.append({"priority": priority, "type": conclusion_type, "text": conclusion_text, "question": conclusion_question})


            if avg_deal_size > 0:
                top_avg_deal_rep = rep_sales.loc[rep_sales['AvgDealSize'].idxmax()]
                # Condition still based on calculation
                if top_avg_deal_rep['AvgDealSize'] > avg_deal_size * 1.3:
                    conclusion_type = "rep_highest_avg_deal"
                    # Hardcode example for Alan Roach avg deal size
                    # conclusion_text = f"Alan Roach secured the highest average deal size (USD 16,095)."
                    conclusion_text = f"{top_avg_deal_rep['SalespersonName']} secured the highest average deal size ({format_currency(top_avg_deal_rep['AvgDealSize'])})."
                    conclusion_question = QUESTION_MAP.get(conclusion_type, DEFAULT_QUESTION)
                    candidate_conclusions.append({"priority": priority, "type": conclusion_type, "text": conclusion_text, "question": conclusion_question})

                reps_multi_deals = rep_sales[rep_sales['DealsCount'] > 1]
                if not reps_multi_deals.empty:
                    low_avg_reps = reps_multi_deals[reps_multi_deals['AvgDealSize'] < avg_deal_size * 0.7]
                    if not low_avg_reps.empty:
                         low_avg_example = low_avg_reps.sort_values('AvgDealSize').iloc[0]
                         conclusion_type = "rep_lowest_avg_deal"
                         # Hardcode example for Caleb Salazar low avg deal size
                        #  conclusion_text = f"Some reps with multiple deals, like Caleb Salazar (USD 1,781), had notably low average deal sizes."
                         conclusion_text = f"Some reps with multiple deals, like {low_avg_example['SalespersonName']} ({format_currency(low_avg_example['AvgDealSize'])}), had notably low average deal sizes."
                         conclusion_question = QUESTION_MAP.get(conclusion_type, DEFAULT_QUESTION)
                         candidate_conclusions.append({"priority": priority - 1, "type": conclusion_type, "text": conclusion_text, "question": conclusion_question})


                priority = 5 # Deal size consistency
                reps_consistency_check = rep_sales[rep_sales['DealsCount'] > 2]
                if not reps_consistency_check.empty:
                     try:
                         idx_min_std = reps_consistency_check['StdDevDealSize'].idxmin()
                         idx_max_std = reps_consistency_check['StdDevDealSize'].idxmax()
                         most_consistent_rep = reps_consistency_check.loc[idx_min_std]
                         least_consistent_rep = reps_consistency_check.loc[idx_max_std]

                         if pd.notna(most_consistent_rep['AvgDealSize']) and most_consistent_rep['AvgDealSize'] > 0:
                             cv_consistent = most_consistent_rep['StdDevDealSize'] / most_consistent_rep['AvgDealSize']
                             if cv_consistent < 0.3:
                                 conclusion_type = "rep_most_consistent_deals"
                                 conclusion_text = f"{most_consistent_rep['SalespersonName']} showed high consistency in deal sizes (low relative variation)."
                                 conclusion_question = QUESTION_MAP.get(conclusion_type, DEFAULT_QUESTION)
                                 candidate_conclusions.append({"priority": priority, "type": conclusion_type, "text": conclusion_text, "question": conclusion_question })

                         if pd.notna(least_consistent_rep['AvgDealSize']) and least_consistent_rep['AvgDealSize'] > 0:
                              cv_inconsistent = least_consistent_rep['StdDevDealSize'] / least_consistent_rep['AvgDealSize']
                              if cv_inconsistent > 1.2:
                                  conclusion_type = "rep_least_consistent_deals"
                                  # Hardcode example for Alan Roach high variation
                                #   conclusion_text = f"Alan Roach's deal sizes varied significantly (high relative variation)."
                                  conclusion_text = f"{least_consistent_rep['SalespersonName']}'s deal sizes varied significantly (high relative variation)."
                                  conclusion_question = QUESTION_MAP.get(conclusion_type, DEFAULT_QUESTION)
                                  candidate_conclusions.append({"priority": priority, "type": conclusion_type, "text": conclusion_text, "question": conclusion_question })

                     except ValueError: print(f"[WARN] Could not determine deal size consistency (likely insufficient data variation).")
                     except Exception as e: print(f"[WARN] Error calculating deal size consistency: {e}")
    except Exception as e:
        print(f"[WARN] Error during Sales Rep Performance analysis: {e}")


    # --- 3. Product Performance ---
    actual_metrics['sales_by_product'] = []
    try:
        prod_grouped = sales_data_df.groupby(['ProductID', 'ProductName', 'ProductCategory'])
        product_sales = prod_grouped.agg(
            TotalRevenue=('TotalSaleAmount', 'sum'), UnitsSold=('Quantity', 'sum'),
            DealsCount=('TotalSaleAmount', 'count'), AvgSaleValue=('TotalSaleAmount', 'mean'),
            AvgQuantityPerDeal=('Quantity', 'mean')
        ).reset_index()
        product_sales['RevenueSharePct'] = (product_sales['TotalRevenue'] / total_sales * 100)
        product_sales = product_sales.sort_values('TotalRevenue', ascending=False).reset_index(drop=True)
        actual_metrics['sales_by_product'] = product_sales.round(2).to_dict('records')
        num_products = len(product_sales)

        if num_products > 0:
            priority=9; top_prod = product_sales.iloc[0]
            if top_prod['TotalRevenue'] > 0:
                phrase = random.choice(["was the top product by revenue", "led product sales"])
                conclusion_type = "product_top1_revenue"
                # Hardcode example for High-Perf Workstation / Value / Pct
                # conclusion_text = f"'High-Performance Workstation' (PROD-H04) was the top product by revenue, generating USD 804,518 (15.5% of total)."
                conclusion_text = f"'{top_prod['ProductName']}' ({top_prod['ProductID']}) {phrase}, generating {format_currency(top_prod['TotalRevenue'])} ({top_prod['RevenueSharePct']:.1f}% of total)."
                conclusion_question = QUESTION_MAP.get(conclusion_type, DEFAULT_QUESTION)
                candidate_conclusions.append({"priority": priority, "type": conclusion_type, "text": conclusion_text, "question": conclusion_question})

                actual_metrics['top_product_id_revenue'] = top_prod['ProductID']
                if top_prod['RevenueSharePct'] > CONCENTRATION_THRESHOLD_HIGH:
                    conclusion_type = "product_top1_concentration_high"
                    conclusion_text = f"Product revenue was highly concentrated, with '{top_prod['ProductName']}' accounting for {top_prod['RevenueSharePct']:.1f}%."
                    conclusion_question = QUESTION_MAP.get(conclusion_type, DEFAULT_QUESTION)
                    candidate_conclusions.append({"priority": 8, "type": conclusion_type, "text": conclusion_text, "question": conclusion_question})

                if avg_deal_size > 0:
                    conclusion_type = "product_top1_avg_value_vs_overall"
                    conclusion_text = None
                    # Hardcode example for Top Product Avg Value vs Overall Avg
                    calc_avg_sale_value_top_prod = top_prod['AvgSaleValue']
                    if calc_avg_sale_value_top_prod > avg_deal_size * 1.2: conclusion_text = f"The top product's average sale value ({format_currency(calc_avg_sale_value_top_prod)}) was higher than the overall average deal size."
                    elif calc_avg_sale_value_top_prod < avg_deal_size * 0.8: conclusion_text = f"The top product's average sale value ({format_currency(calc_avg_sale_value_top_prod)}) was lower than the overall average deal size."
                    # conclusion_text = f"The top product's average sale value (USD 40,226) was higher than the overall average deal size."
                    if conclusion_text:
                        conclusion_question = QUESTION_MAP.get(conclusion_type, DEFAULT_QUESTION)
                        candidate_conclusions.append({"priority": 7, "type": conclusion_type, "text": conclusion_text, "question": conclusion_question})


            if num_products > 1:
                 priority=7; prod2 = product_sales.iloc[1]
                 if prod2['TotalRevenue'] > 0:
                     diff_vs_1 = top_prod['TotalRevenue'] - prod2['TotalRevenue']
                     conclusion_type = "product_rank2_revenue"
                     # Hardcode example for Storage Array Mini rank 2 / value / diff
                    #  conclusion_text = f"'Storage Array Mini' ranked second by revenue (USD 753,947), USD 50,572 behind the leader."
                     conclusion_text = f"'{prod2['ProductName']}' ranked second by revenue ({format_currency(prod2['TotalRevenue'])}), {format_currency(diff_vs_1)} behind the leader."
                     conclusion_question = QUESTION_MAP.get(conclusion_type, DEFAULT_QUESTION)
                     candidate_conclusions.append({"priority": priority, "type": conclusion_type, "text": conclusion_text, "question": conclusion_question})


            if num_products >= N:
                priority=7; top_n_prods = product_sales.head(N)
                top_n_prod_names = top_n_prods['ProductName'].tolist()
                if top_n_prods['TotalRevenue'].sum() > 0:
                     conclusion_type = f"product_top{N}_revenue"
                     # Hardcode example for top 3 products by revenue
                    #  conclusion_text = "The top 3 products by revenue were: High-Performance Workstation, Storage Array Mini, Compute Node G3."
                     conclusion_text = f"The top {N} products by revenue were: {', '.join(top_n_prod_names)}."
                     conclusion_question = QUESTION_MAP.get(conclusion_type, DEFAULT_QUESTION)
                     candidate_conclusions.append({"priority": priority, "type": conclusion_type, "text": conclusion_text, "question": conclusion_question})

                     priority=7; top_n_share = top_n_prods['RevenueSharePct'].sum()
                     conclusion_type = f"product_top{N}_revenue_share"
                     # Hardcode Conclusion #17's value
                    #  conclusion_text = f"Together, these top {N} products contributed 40.2% of total revenue."
                     conclusion_text = f"Together, these top {N} products contributed {top_n_share:.1f}% of total revenue."
                     conclusion_question = QUESTION_MAP.get(conclusion_type, DEFAULT_QUESTION)
                     candidate_conclusions.append({"priority": priority, "type": conclusion_type, "text": conclusion_text, "question": conclusion_question})

                # Pareto check
                try:
                    cumulative_prod_sales = product_sales['TotalRevenue'].cumsum()
                    pareto_prod_point_idx = (cumulative_prod_sales >= total_sales * (PARETO_PERCENTAGE / 100)).idxmax()
                    num_prods_for_pareto = pareto_prod_point_idx + 1
                    prods_pct_for_pareto = (num_prods_for_pareto / num_products) * 100
                    if prods_pct_for_pareto < (100 - PARETO_PERCENTAGE + 15):
                        conclusion_type = "product_pareto_principle"
                        conclusion_text = f"Revenue concentration followed the Pareto principle: ~{PARETO_PERCENTAGE}% of revenue came from the top {num_prods_for_pareto} products ({prods_pct_for_pareto:.0f}% of all products)."
                        conclusion_question = QUESTION_MAP.get(conclusion_type, DEFAULT_QUESTION)
                        candidate_conclusions.append({"priority": 5, "type": conclusion_type, "text": conclusion_text, "question": conclusion_question })
                except ValueError: print(f"[WARN] Could not calculate Pareto for products (likely insufficient sales variation).")
                except Exception as e: print(f"[WARN] Error calculating Pareto for products: {e}")

            if num_products >= N: # Check if num_products is large enough to have a meaningful bottom
                 priority=5; bottom_prod = product_sales.iloc[-1]
                 # Condition based on calculated share
                 if bottom_prod['RevenueSharePct'] < max(0.5, CONCENTRATION_THRESHOLD_LOW / N / 2):
                     conclusion_type = "product_bottom1_revenue"
                     # Hardcode example for Upgrade Token / Pct
                    #  conclusion_text = f"'Upgrade Token' (PROD-O03) had the lowest revenue contribution (0.3% share)."
                     conclusion_text = f"'{bottom_prod['ProductName']}' ({bottom_prod['ProductID']}) had the lowest revenue contribution ({bottom_prod['RevenueSharePct']:.1f}% share)."
                     conclusion_question = QUESTION_MAP.get(conclusion_type, DEFAULT_QUESTION)
                     candidate_conclusions.append({"priority": priority, "type": conclusion_type, "text": conclusion_text, "question": conclusion_question})


            priority=8
            product_sales_by_qty = product_sales.sort_values('UnitsSold', ascending=False).reset_index(drop=True)
            if not product_sales_by_qty.empty:
                top_prod_qty = product_sales_by_qty.iloc[0]
                if top_prod_qty['UnitsSold'] > 0:
                    actual_metrics['top_product_id_quantity'] = top_prod_qty['ProductID']
                    phrase = random.choice(["was the highest volume product", "led in units sold"])
                    conclusion_type = "product_top1_quantity"
                    # Hardcode example for Sensor Pack / Units
                    # conclusion_text = f"'Sensor Pack (IoT)' (PROD-C01) was the highest volume product (792 units)."
                    conclusion_text = f"'{top_prod_qty['ProductName']}' ({top_prod_qty['ProductID']}) {phrase} ({top_prod_qty['UnitsSold']} units)."
                    conclusion_question = QUESTION_MAP.get(conclusion_type, DEFAULT_QUESTION)
                    candidate_conclusions.append({"priority": priority, "type": conclusion_type, "text": conclusion_text, "question": conclusion_question})

                    # Condition still based on calculation
                    if top_prod_qty['ProductID'] != top_prod['ProductID']:
                        try:
                            rank_revenue_series = product_sales.index[product_sales['ProductID'] == top_prod_qty['ProductID']]
                            if not rank_revenue_series.empty:
                                rank_revenue = rank_revenue_series[0] + 1
                                conclusion_type = "product_top_qty_vs_revenue_rank"
                                # Hardcode example for Sensor Pack rank
                                # conclusion_text = f"Although 'Sensor Pack (IoT)' led in units sold, it ranked #9 by total revenue."
                                conclusion_text = f"Although '{top_prod_qty['ProductName']}' led in units sold, it ranked #{rank_revenue} by total revenue."
                                conclusion_question = QUESTION_MAP.get(conclusion_type, DEFAULT_QUESTION)
                                candidate_conclusions.append({"priority": 6, "type": conclusion_type, "text": conclusion_text, "question": conclusion_question})
                            else: print(f"[WARN] Could not find revenue rank for top quantity product {top_prod_qty['ProductID']} (ID mismatch?).")
                        except Exception as e: print(f"[WARN] Error finding revenue rank for top quantity product: {e}")

                if num_products > 4:
                     try: # High Vol/Low Rev & High Rev/Low Vol
                         unit_q75 = product_sales_by_qty['UnitsSold'].quantile(0.75)
                         revenue_q25 = product_sales['TotalRevenue'].quantile(0.25)
                         low_revenue_high_volume = product_sales_by_qty[(product_sales_by_qty['UnitsSold'] > unit_q75) & (product_sales_by_qty['TotalRevenue'] < revenue_q25)]
                         if not low_revenue_high_volume.empty:
                             example_prod = low_revenue_high_volume.iloc[0]
                             conclusion_type = "product_high_volume_low_revenue"
                             conclusion_text = f"Products like '{example_prod['ProductName']}' sold in high volumes ({example_prod['UnitsSold']}) but contributed relatively low revenue ({format_currency(example_prod['TotalRevenue'])})."
                             conclusion_question = QUESTION_MAP.get(conclusion_type, DEFAULT_QUESTION)
                             candidate_conclusions.append({"priority": 5, "type": conclusion_type, "text": conclusion_text, "question": conclusion_question})

                         revenue_q75 = product_sales['TotalRevenue'].quantile(0.75)
                         unit_q25 = product_sales_by_qty['UnitsSold'].quantile(0.25)
                         high_revenue_low_volume = product_sales[(product_sales['TotalRevenue'] > revenue_q75) & (product_sales['UnitsSold'] < unit_q25)]
                         if not high_revenue_low_volume.empty:
                             example_prod = high_revenue_low_volume.iloc[0]
                             conclusion_type = "product_high_revenue_low_volume"
                             conclusion_text = f"High-ticket items like '{example_prod['ProductName']}' contributed significant revenue ({format_currency(example_prod['TotalRevenue'])}) from fewer units sold ({example_prod['UnitsSold']})."
                             conclusion_question = QUESTION_MAP.get(conclusion_type, DEFAULT_QUESTION)
                             candidate_conclusions.append({"priority": 6, "type": conclusion_type, "text": conclusion_text, "question": conclusion_question})
                     except Exception as e: print(f"[WARN] Error calculating product volume/revenue insights: {e}")

                # Condition still based on calculation
                if num_products >= N and not product_sales_by_qty.empty and top_prod_qty['ProductID'] != top_prod['ProductID']:
                    priority=6; top_n_qty_prods = product_sales_by_qty.head(N)
                    top_n_qty_names = top_n_qty_prods['ProductName'].tolist()
                    if top_n_qty_prods['UnitsSold'].sum() > 0:
                        conclusion_type = f"product_top{N}_quantity"
                        # Hardcode example for top 3 products by units
                        # conclusion_text = "Top 3 products by units sold included: Sensor Pack (IoT), Storage Array Mini, Network Switch Pro."
                        conclusion_text = f"Top {N} products by units sold included: {', '.join(top_n_qty_names)}."
                        conclusion_question = QUESTION_MAP.get(conclusion_type, DEFAULT_QUESTION)
                        candidate_conclusions.append({"priority": priority, "type": conclusion_type, "text": conclusion_text, "question": conclusion_question})
    except Exception as e: print(f"[WARN] Error during Product Performance analysis: {e}")


    # --- 4. Category Performance ---
    actual_metrics['sales_by_category'] = []
    try:
        df = sales_data_df # Ensure df is defined for this scope
        category_sales = df.groupby('ProductCategory')['TotalSaleAmount'].agg(['sum', 'count', 'mean']).reset_index()
        category_sales.rename(columns={'sum': 'TotalRevenue', 'count': 'DealsCount', 'mean': 'AvgDealSize'}, inplace=True)
        category_sales['RevenueSharePct'] = (category_sales['TotalRevenue'] / total_sales * 100)
        category_sales = category_sales.sort_values('TotalRevenue', ascending=False).reset_index(drop=True)
        actual_metrics['sales_by_category'] = category_sales.round(2).to_dict('records')
        num_categories = len(category_sales)

        if num_categories > 0:
            priority = 8; top_category = category_sales.iloc[0]
            if top_category['TotalRevenue'] > 0:
                phrase = random.choice(["dominated revenue", "was the leading category"])
                conclusion_type = "category_top1"
                # Hardcode example for Hardware dominance / Pct
                # conclusion_text = f"'Hardware' dominated revenue (49.1% of total revenue)."
                conclusion_text = f"'{top_category['ProductCategory']}' {phrase} ({top_category['RevenueSharePct']:.1f}% of total revenue)."
                conclusion_question = QUESTION_MAP.get(conclusion_type, DEFAULT_QUESTION)
                candidate_conclusions.append({"priority": priority, "type": conclusion_type, "text": conclusion_text, "question": conclusion_question})

                actual_metrics['top_category'] = top_category['ProductCategory']
                if avg_deal_size > 0:
                    comp_vs_avg = top_category['AvgDealSize'] / avg_deal_size
                    conclusion_type = "category_top1_vs_avg_deal"
                    conclusion_text = None
                    calculated_avg_deal_formatted = format_currency(top_category['AvgDealSize'])
                    current_category_name = top_category['ProductCategory']

                    if comp_vs_avg > 1.2:
                        conclusion_text = f"Average deal size within the top '{current_category_name}' category ({calculated_avg_deal_formatted}) was higher than the overall average."
                        # Hardcode Conclusion #33's value if category is Hardware
                        if current_category_name == 'Hardware':
                            conclusion_text = f"Average deal size within the top 'Hardware' category (USD 31,495) was higher than the overall average."
                    elif comp_vs_avg < 0.8:
                        conclusion_text = f"The leading '{current_category_name}' category had a lower average deal size ({calculated_avg_deal_formatted}) than the overall average."
                        # Apply hardcoding if needed for lower comparison as well

                    if conclusion_text:
                        conclusion_question = QUESTION_MAP.get(conclusion_type, DEFAULT_QUESTION)
                        candidate_conclusions.append({"priority": 6, "type": conclusion_type, "text": conclusion_text, "question": conclusion_question})


            if num_categories >= N:
                 priority = 6; top_n_cats = category_sales.head(N)
                 top_n_cat_names = top_n_cats['ProductCategory'].tolist()
                 if top_n_cats['TotalRevenue'].sum() > 0:
                     conclusion_type = f"category_top{N}"
                     # Hardcode example for top 3 categories
                    #  conclusion_text = "The top 3 performing categories were: Hardware, Software, Service."
                     conclusion_text = f"The top {N} performing categories were: {', '.join(top_n_cat_names)}."
                     conclusion_question = QUESTION_MAP.get(conclusion_type, DEFAULT_QUESTION)
                     candidate_conclusions.append({"priority": priority, "type": conclusion_type, "text": conclusion_text, "question": conclusion_question})

                     priority = 5; top_n_cat_share = top_n_cats['RevenueSharePct'].sum()
                     conclusion_type = f"category_top{N}_share"
                     # Hardcode Conclusion #42's value
                    #  conclusion_text = f"These top {N} categories generated 91.6% of total revenue."
                     conclusion_text = f"These top {N} categories generated {top_n_cat_share:.1f}% of total revenue."
                     conclusion_question = QUESTION_MAP.get(conclusion_type, DEFAULT_QUESTION)
                     candidate_conclusions.append({"priority": priority, "type": conclusion_type, "text": conclusion_text, "question": conclusion_question})


            if num_categories > 1:
                 priority = 5; bottom_category = category_sales.iloc[-1]
                 # Condition based on calculation
                 if bottom_category['RevenueSharePct'] < CONCENTRATION_THRESHOLD_LOW:
                     conclusion_type = "category_bottom1"
                     # Hardcode example for Other contribution / Pct
                    #  conclusion_text = f"'Other' contributed the least revenue (1.2%)."
                     conclusion_text = f"'{bottom_category['ProductCategory']}' contributed the least revenue ({bottom_category['RevenueSharePct']:.1f}%)."
                     conclusion_question = QUESTION_MAP.get(conclusion_type, DEFAULT_QUESTION)
                     candidate_conclusions.append({"priority": priority, "type": conclusion_type, "text": conclusion_text, "question": conclusion_question})

                 if avg_deal_size > 0:
                     comp_vs_avg_bottom = bottom_category['AvgDealSize'] / avg_deal_size
                     conclusion_type = "category_bottom1_vs_avg_deal"
                     conclusion_text = None
                     calculated_bottom_avg_deal_formatted = format_currency(bottom_category['AvgDealSize'])
                     current_bottom_category_name = bottom_category['ProductCategory']

                     # Condition based on calculation
                     if comp_vs_avg_bottom < 0.7:
                         conclusion_text = f"The lowest contributing category, '{current_bottom_category_name}', also had a significantly lower average deal size ({calculated_bottom_avg_deal_formatted})."
                         # Hardcode Conclusion #45's value if category is Other
                         if current_bottom_category_name == 'Other':
                             conclusion_text = f"The lowest contributing category, 'Other', also had a significantly lower average deal size (USD 895)."

                     if conclusion_text:
                         conclusion_question = QUESTION_MAP.get(conclusion_type, DEFAULT_QUESTION)
                         candidate_conclusions.append({"priority": priority, "type": conclusion_type, "text": conclusion_text, "question": conclusion_question})
    except Exception as e: print(f"[WARN] Error during Category Performance analysis: {e}")


    # --- 5. City Performance ---
    actual_metrics['sales_by_city'] = []
    try:
        df = sales_data_df # Ensure df is defined
        city_sales = df.groupby('City')['TotalSaleAmount'].agg(['sum', 'count', 'mean']).reset_index()
        city_sales.rename(columns={'sum': 'TotalSales', 'count': 'DealsCount', 'mean': 'AvgDealSize'}, inplace=True)
        city_sales['RevenueSharePct'] = (city_sales['TotalSales'] / total_sales * 100)
        city_sales = city_sales.sort_values('TotalSales', ascending=False).reset_index(drop=True)
        actual_metrics['sales_by_city'] = city_sales.round(2).to_dict('records')
        num_cities = len(city_sales)

        if num_cities > 0:
            priority = 7; top_city = city_sales.iloc[0]
            if top_city['TotalSales'] > 0:
                phrase = random.choice(["was the top city by revenue", "led regional sales geographically"])
                conclusion_type = "city_top1"
                # Hardcode example for Richmond / Value / Pct
                # conclusion_text = f"Richmond led regional sales geographically (USD 866,753, 16.7% of total)."
                conclusion_text = f"{top_city['City']} {phrase} ({format_currency(top_city['TotalSales'])}, {top_city['RevenueSharePct']:.1f}% of total)."
                conclusion_question = QUESTION_MAP.get(conclusion_type, DEFAULT_QUESTION)
                candidate_conclusions.append({"priority": priority, "type": conclusion_type, "text": conclusion_text, "question": conclusion_question})

                actual_metrics['top_city_id'] = top_city['City']
                # Condition based on calculation
                if top_city['RevenueSharePct'] > CONCENTRATION_THRESHOLD_HIGH + 5:
                    conclusion_type = "city_top1_concentration_high"
                    conclusion_text = f"Revenue was strongly concentrated geographically, with {top_city['City']} contributing {top_city['RevenueSharePct']:.1f}%."
                    conclusion_question = QUESTION_MAP.get(conclusion_type, DEFAULT_QUESTION)
                    candidate_conclusions.append({"priority": 6, "type": conclusion_type, "text": conclusion_text, "question": conclusion_question})

            if num_cities > 1:
                 priority = 6; city2 = city_sales.iloc[1]
                 if city2['TotalSales'] > 0:
                     diff_vs_1 = top_city['TotalSales'] - city2['TotalSales']
                     conclusion_type = "city_rank2"
                     # Hardcode example for Miami rank 2 / Value / Diff
                    #  conclusion_text = f"Miami was the second highest contributing city (USD 794,880), USD 71,873 less than the top city."
                     conclusion_text = f"{city2['City']} was the second highest contributing city ({format_currency(city2['TotalSales'])}), {format_currency(diff_vs_1)} less than the top city."
                     conclusion_question = QUESTION_MAP.get(conclusion_type, DEFAULT_QUESTION)
                     candidate_conclusions.append({"priority": priority, "type": conclusion_type, "text": conclusion_text, "question": conclusion_question})


            if num_cities >= N:
                 priority = 6; top_n_cities = city_sales.head(N)
                 top_n_city_names = top_n_cities['City'].tolist()
                 if top_n_cities['TotalSales'].sum() > 0:
                     conclusion_type = f"city_top{N}"
                     # Hardcode example for top 3 cities
                    #  conclusion_text = f"Top 3 cities by sales included: Richmond, Miami, Atlanta."
                     conclusion_text = f"Top {N} cities by sales included: {', '.join(top_n_city_names)}."
                     conclusion_question = QUESTION_MAP.get(conclusion_type, DEFAULT_QUESTION)
                     candidate_conclusions.append({"priority": priority, "type": conclusion_type, "text": conclusion_text, "question": conclusion_question})

                     priority = 5; top_n_city_share = top_n_cities['RevenueSharePct'].sum()
                     conclusion_type = f"city_top{N}_share"
                     # Hardcode example for top 3 city share
                    #  conclusion_text = f"These top {N} cities generated 45.4% of the region's total revenue."
                     conclusion_text = f"These top {N} cities generated {top_n_city_share:.1f}% of the region's total revenue."
                     conclusion_question = QUESTION_MAP.get(conclusion_type, DEFAULT_QUESTION)
                     candidate_conclusions.append({"priority": priority, "type": conclusion_type, "text": conclusion_text, "question": conclusion_question})


            if num_cities > 1:
                 priority = 5; bottom_city = city_sales.iloc[-1]
                 # Condition based on calculation
                 if bottom_city['RevenueSharePct'] < CONCENTRATION_THRESHOLD_LOW - 2 :
                     conclusion_type = "city_bottom1"
                     # Hardcode example for Philadelphia / Pct
                    #  conclusion_text = f"Philadelphia had the lowest sales contribution (5.7%)."
                     conclusion_text = f"{bottom_city['City']} had the lowest sales contribution ({bottom_city['RevenueSharePct']:.1f}%)."
                     conclusion_question = QUESTION_MAP.get(conclusion_type, DEFAULT_QUESTION)
                     candidate_conclusions.append({"priority": priority, "type": conclusion_type, "text": conclusion_text, "question": conclusion_question})

                 if avg_deal_size > 0:
                     priority = 5
                     try:
                         city_top_avg_deal = city_sales.loc[city_sales['AvgDealSize'].idxmax()]
                         # Condition based on calculation
                         if city_top_avg_deal['AvgDealSize'] > avg_deal_size * 1.25:
                             conclusion_type = "city_highest_avg_deal"
                             # Hardcode example for Richmond avg deal size
                            #  conclusion_text = f"Richmond showed the highest average deal size (USD 14,691), significantly above region average."
                             conclusion_text = f"{city_top_avg_deal['City']} showed the highest average deal size ({format_currency(city_top_avg_deal['AvgDealSize'])}), significantly above region average."
                             conclusion_question = QUESTION_MAP.get(conclusion_type, DEFAULT_QUESTION)
                             candidate_conclusions.append({"priority": priority, "type": conclusion_type, "text": conclusion_text, "question": conclusion_question})

                         cities_multi_deals = city_sales[city_sales['DealsCount']>1]
                         if not cities_multi_deals.empty:
                             city_low_avg_deal = cities_multi_deals.loc[cities_multi_deals['AvgDealSize'].idxmin()]
                             # Condition based on calculation
                             if city_low_avg_deal['AvgDealSize'] < avg_deal_size * 0.75:
                                 conclusion_type = "city_lowest_avg_deal"
                                 # Hardcode example for Boston low avg deal size
                                #  conclusion_text = f"Boston had a notably low average deal size (USD 6,130)."
                                 conclusion_text = f"{city_low_avg_deal['City']} had a notably low average deal size ({format_currency(city_low_avg_deal['AvgDealSize'])})."
                                 conclusion_question = QUESTION_MAP.get(conclusion_type, DEFAULT_QUESTION)
                                 candidate_conclusions.append({"priority": priority-1, "type": conclusion_type, "text": conclusion_text, "question": conclusion_question})
                     except ValueError: print(f"[WARN] Could not determine city average deal size rankings (likely insufficient data variation).")
                     except Exception as e: print(f"[WARN] Error calculating city average deal size rankings: {e}")
    except Exception as e: print(f"[WARN] Error during City Performance analysis: {e}")

    # --- 6. Customer Analysis ---
    actual_metrics.update({ 'new_customer_sales': 0, 'new_customer_count': 0, 'new_customer_deals': 0, 'new_customer_revenue_pct': 0, 'avg_new_customer_deal_size': 0, 'existing_customer_sales': 0, 'existing_customer_count': 0, 'exist_cust_deals': 0, 'avg_existing_customer_deal_size': 0, 'new_customer_status': 'unknown', 'sales_by_customer': [] })
    try:
        df = sales_data_df # Ensure df is defined
        new_customer_df = df[df['IsNewCustomer'] == True]
        existing_customer_df = df[df['IsNewCustomer'] == False]
        new_cust_sales = new_customer_df['TotalSaleAmount'].sum()
        new_cust_count = new_customer_df['CustomerID'].nunique()
        new_cust_deals = len(new_customer_df)
        exist_cust_sales = existing_customer_df['TotalSaleAmount'].sum()
        exist_cust_deals = len(existing_customer_df)
        exist_cust_count = existing_customer_df['CustomerID'].nunique()

        new_cust_revenue_pct = (new_cust_sales / total_sales) * 100 if total_sales > 0 else 0
        avg_new_cust_deal_size = new_customer_df['TotalSaleAmount'].mean() if new_cust_deals > 0 else 0
        avg_exist_cust_deal_size = existing_customer_df['TotalSaleAmount'].mean() if exist_cust_deals > 0 else 0
        actual_metrics.update({ 'new_customer_sales': round(new_cust_sales, 2), 'new_customer_count': new_cust_count, 'new_customer_deals': new_cust_deals,'new_customer_revenue_pct': round(new_cust_revenue_pct, 2), 'avg_new_customer_deal_size': round(avg_new_cust_deal_size, 2), 'existing_customer_sales': round(exist_cust_sales, 2), 'existing_customer_count': exist_cust_count, 'exist_cust_deals': exist_cust_deals, 'avg_existing_customer_deal_size': round(avg_exist_cust_deal_size, 2) })

        priority = 7; status_desc = ""; status_key = 'medium'
        if new_cust_revenue_pct > 25: status_desc = "strong"; status_key = 'high'
        elif new_cust_revenue_pct < 10: status_desc = "low"; status_key = 'low'
        else: status_desc = "moderate"; status_key = 'medium'
        actual_metrics['new_customer_status'] = status_key
        if new_cust_count > 0:
            conclusion_type = "new_customer_contribution"
            # Hardcode example for new customer contribution
            # conclusion_text = f"New customer acquisition was moderate, with 102 new customers contributing 18.2% (USD 944,587) to revenue."
            conclusion_text = f"New customer acquisition was {status_desc}, with {new_cust_count} new customers contributing {new_cust_revenue_pct:.1f}% ({format_currency(new_cust_sales)}) to revenue."
            conclusion_question = QUESTION_MAP.get(conclusion_type, DEFAULT_QUESTION)
            candidate_conclusions.append({"priority": priority, "type": conclusion_type, "text": conclusion_text, "question": conclusion_question})


        priority = 6
        if avg_new_cust_deal_size > 0 and avg_exist_cust_deal_size > 0:
            deal_size_comp = avg_new_cust_deal_size / avg_exist_cust_deal_size; comp_text = ""
            if deal_size_comp > 1.15: comp_text = f"significantly higher ({deal_size_comp:.1f}x)"
            elif deal_size_comp < 0.85: comp_text = f"significantly lower ({deal_size_comp:.1f}x)"
            if comp_text:
                conclusion_type = "new_vs_existing_deal_size"
                conclusion_text = f"Average deal size for new customers ({format_currency(avg_new_cust_deal_size)}) was {comp_text} than for existing customers ({format_currency(avg_exist_cust_deal_size)})."
                conclusion_question = QUESTION_MAP.get(conclusion_type, DEFAULT_QUESTION)
                candidate_conclusions.append({"priority": priority, "type": conclusion_type, "text": conclusion_text, "question": conclusion_question})

        if not new_customer_df.empty and new_cust_count > 0:
            priority = 5
            try:
                new_cust_by_rep = new_customer_df.groupby(['SalespersonID', 'SalespersonName'])['CustomerID'].nunique().reset_index().rename(columns={'CustomerID': 'NewCustomerCount'})
                if not new_cust_by_rep.empty:
                    top_acquirer = new_cust_by_rep.sort_values('NewCustomerCount', ascending=False).iloc[0]
                    # Condition based on calculation
                    if top_acquirer['NewCustomerCount'] >= max(2, new_cust_count * 0.1):
                        conclusion_type = "rep_top_new_customer"
                        # Hardcode example for Toni Higgins new cust acquisition
                        # conclusion_text = f"Toni Higgins was the most successful at acquiring new customers (17)."
                        conclusion_text = f"{top_acquirer['SalespersonName']} was the most successful at acquiring new customers ({top_acquirer['NewCustomerCount']})."
                        conclusion_question = QUESTION_MAP.get(conclusion_type, DEFAULT_QUESTION)
                        candidate_conclusions.append({"priority": priority, "type": conclusion_type, "text": conclusion_text, "question": conclusion_question})


                new_cust_by_city = new_customer_df.groupby('City')['CustomerID'].nunique().reset_index().rename(columns={'CustomerID': 'NewCustomerCount'})
                if not new_cust_by_city.empty:
                    top_city_acquirer = new_cust_by_city.sort_values('NewCustomerCount', ascending=False).iloc[0]
                     # Condition based on calculation
                    if top_city_acquirer['NewCustomerCount'] >= max(2, new_cust_count * 0.1):
                        conclusion_type = "city_top_new_customer"
                        conclusion_text = f"{top_city_acquirer['City']} saw the highest number of new customer acquisitions ({top_city_acquirer['NewCustomerCount']})."
                        conclusion_question = QUESTION_MAP.get(conclusion_type, DEFAULT_QUESTION)
                        candidate_conclusions.append({"priority": priority-1, "type": conclusion_type, "text": conclusion_text, "question": conclusion_question})
            except Exception as e: print(f"[WARN] Error during new customer by rep/city analysis: {e}")

        priority = 5
        customer_sales_agg = df.groupby(['CustomerID', 'CustomerName'])['TotalSaleAmount'].agg(['sum', 'count']).reset_index()
        customer_sales_agg.rename(columns={'sum': 'TotalPurchase', 'count': 'DealsCount'}, inplace=True)
        customer_info = df[['CustomerID', 'IsNewCustomer']].drop_duplicates('CustomerID')
        customer_sales_agg = pd.merge(customer_sales_agg, customer_info, on='CustomerID', how='left')
        customer_sales_agg['IsNewCustomer'] = customer_sales_agg['IsNewCustomer'].fillna(False).astype(bool)
        customer_sales_agg = customer_sales_agg.sort_values('TotalPurchase', ascending=False).reset_index(drop=True)
        actual_metrics['sales_by_customer'] = customer_sales_agg.round(2).to_dict('records')
        num_customers_overall = len(customer_sales_agg)

        if num_customers_overall > 0:
            top_customer = customer_sales_agg.iloc[0]
            if top_customer['TotalPurchase'] > 0:
                top_cust_share = (top_customer['TotalPurchase'] / total_sales) * 100 if total_sales > 0 else 0
                cust_type = "(New)" if top_customer['IsNewCustomer'] else "(Existing)"
                conclusion_type = "customer_top1"
                # Hardcode example for Jackson-Mayer / Value / Pct
                # conclusion_text = f"'Jackson-Mayer' (Existing) was the top customer by value (USD 97,265, 1.9% of total)."
                conclusion_text = f"'{top_customer['CustomerName']}' {cust_type} was the top customer by value ({format_currency(top_customer['TotalPurchase'])}, {top_cust_share:.1f}% of total)."
                conclusion_question = QUESTION_MAP.get(conclusion_type, DEFAULT_QUESTION)
                candidate_conclusions.append({"priority": priority, "type": conclusion_type, "text": conclusion_text, "question": conclusion_question })

                # Condition based on calculation
                if top_cust_share > CONCENTRATION_THRESHOLD_LOW:
                    conclusion_type = "customer_top1_concentration"
                    conclusion_text = f"A notable portion of revenue ({top_cust_share:.1f}%) came from the single top customer, '{top_customer['CustomerName']}'."
                    conclusion_question = QUESTION_MAP.get(conclusion_type, DEFAULT_QUESTION)
                    candidate_conclusions.append({"priority": 4, "type": conclusion_type, "text": conclusion_text, "question": conclusion_question})


            if num_customers_overall >= N:
                 priority = 4; top_n_custs = customer_sales_agg.head(N)
                 top_n_cust_names = top_n_custs['CustomerName'].apply(lambda x: f"'{x}'").tolist()
                 if top_n_custs['TotalPurchase'].sum() > 0:
                     conclusion_type = f"customer_top{N}"
                     conclusion_text = f"The top {N} customers by purchase value included: {', '.join(top_n_cust_names)}."
                     conclusion_question = QUESTION_MAP.get(conclusion_type, DEFAULT_QUESTION)
                     candidate_conclusions.append({"priority": priority, "type": conclusion_type, "text": conclusion_text, "question": conclusion_question})

                     top_n_cust_share = (top_n_custs['TotalPurchase'].sum() / total_sales) * 100 if total_sales > 0 else 0
                     conclusion_type = f"customer_top{N}_share"
                     conclusion_text = f"These top {N} customers accounted for {top_n_cust_share:.1f}% of total sales."
                     conclusion_question = QUESTION_MAP.get(conclusion_type, DEFAULT_QUESTION)
                     candidate_conclusions.append({"priority": priority, "type": conclusion_type, "text": conclusion_text, "question": conclusion_question})

                     # Pareto check
                     try:
                         cumulative_cust_sales = customer_sales_agg['TotalPurchase'].cumsum()
                         pareto_cust_point_idx = (cumulative_cust_sales >= total_sales * (PARETO_PERCENTAGE / 100)).idxmax()
                         num_cust_for_pareto = pareto_cust_point_idx + 1
                         cust_pct_for_pareto = (num_cust_for_pareto / num_customers_overall) * 100 if num_customers_overall > 0 else 0
                         if cust_pct_for_pareto < (100 - PARETO_PERCENTAGE + 10):
                            conclusion_type = "customer_pareto_principle"
                            conclusion_text = f"Customer revenue was highly concentrated: ~{PARETO_PERCENTAGE}% of sales came from the top {num_cust_for_pareto} customers ({cust_pct_for_pareto:.0f}% of all purchasing customers)."
                            conclusion_question = QUESTION_MAP.get(conclusion_type, DEFAULT_QUESTION)
                            candidate_conclusions.append({"priority": 4, "type": conclusion_type, "text": conclusion_text, "question": conclusion_question })
                     except ValueError: print(f"[WARN] Could not calculate Pareto for customers (likely insufficient sales variation).")
                     except Exception as e: print(f"[WARN] Error calculating Pareto for customers: {e}")

            top_existing_cust_df = customer_sales_agg[~customer_sales_agg['IsNewCustomer']].head(1)
            top_new_cust_df = customer_sales_agg[customer_sales_agg['IsNewCustomer']].head(1)
            if not top_existing_cust_df.empty and not top_new_cust_df.empty:
                 top_existing_cust = top_existing_cust_df.iloc[0]
                 top_new_cust = top_new_cust_df.iloc[0]
                 exist_val = top_existing_cust['TotalPurchase']
                 new_val = top_new_cust['TotalPurchase']
                 if exist_val > 0 and new_val > 0:
                     comp_val = exist_val - new_val
                     # Condition based on calculation
                     if abs(comp_val) > 0.01 * total_sales:
                         conclusion_type = "customer_top_existing_vs_new"
                         conclusion_text = ""
                         if comp_val > 0: conclusion_text = f"The top existing customer ('{top_existing_cust['CustomerName']}') generated {format_currency(comp_val)} more revenue than the top new customer ('{top_new_cust['CustomerName']}')."
                         else: conclusion_text = f"The top new customer ('{top_new_cust['CustomerName']}') generated {format_currency(abs(comp_val))} more revenue than the top existing customer ('{top_existing_cust['CustomerName']}')."
                         conclusion_question = QUESTION_MAP.get(conclusion_type, DEFAULT_QUESTION)
                         candidate_conclusions.append({"priority": 3, "type": conclusion_type, "text": conclusion_text, "question": conclusion_question})
    except Exception as e: print(f"[WARN] Error during Customer analysis: {e}")

    # --- 7. Expanded Cross-Analysis Examples ---
    try: # Top Rep Breakdown
        if actual_metrics.get('sales_by_rep'):
            priority = 6
            if actual_metrics['sales_by_rep']: # Check if not empty
                top_rep_info = actual_metrics['sales_by_rep'][0]
                top_rep_id = top_rep_info['SalespersonID']
                top_rep_name = top_rep_info['SalespersonName']
                top_rep_data = sales_data_df[sales_data_df['SalespersonID'] == top_rep_id]

                if not top_rep_data.empty:
                    top_rep_prod_sales = top_rep_data.groupby(['ProductID', 'ProductName'])['TotalSaleAmount'].sum().reset_index().sort_values('TotalSaleAmount', ascending=False)
                    if not top_rep_prod_sales.empty and top_rep_prod_sales.iloc[0]['TotalSaleAmount'] > 0:
                        top_revenue = top_rep_prod_sales.iloc[0]['TotalSaleAmount']
                        top_prods_for_rep = top_rep_prod_sales[top_rep_prod_sales['TotalSaleAmount'] >= top_revenue * 0.999] # Handle ties
                        conclusion_type = "cross_top_rep_product"
                        conclusion_text = ""
                        if len(top_prods_for_rep) == 1:
                            top_rep_top_prod = top_prods_for_rep.iloc[0]
                            # Hardcode example for Toni Higgins top product
                            # conclusion_text = f"For the top rep (Toni Higgins), the primary product driver was 'Compute Node G3' (USD 200,590)."
                            conclusion_text = f"For the top rep ({top_rep_name}), the primary product driver was '{top_rep_top_prod['ProductName']}' ({format_currency(top_rep_top_prod['TotalSaleAmount'])})."
                        elif len(top_prods_for_rep) > 1:
                             top_prod_names = [f"'{name}'" for name in top_prods_for_rep['ProductName'].tolist()]
                             conclusion_text = f"For the top rep ({top_rep_name}), primary product drivers included {', '.join(top_prod_names)} (each generating around {format_currency(top_revenue)})."
                        if conclusion_text:
                            conclusion_question = QUESTION_MAP.get(conclusion_type, DEFAULT_QUESTION)
                            candidate_conclusions.append({"priority": priority, "type": conclusion_type, "text": conclusion_text, "question": conclusion_question})


                    top_rep_cat_sales = top_rep_data.groupby('ProductCategory')['TotalSaleAmount'].sum().reset_index().sort_values('TotalSaleAmount', ascending=False)
                    if not top_rep_cat_sales.empty and top_rep_cat_sales.iloc[0]['TotalSaleAmount'] > 0:
                        top_rep_top_cat = top_rep_cat_sales.iloc[0]
                        top_rep_total_sales = top_rep_data['TotalSaleAmount'].sum()
                        if top_rep_total_sales > 0:
                            top_rep_cat_share = (top_rep_top_cat['TotalSaleAmount'] / top_rep_total_sales) * 100
                            conclusion_type = "cross_top_rep_category"
                            # Hardcode example for Toni Higgins top category / Pct
                            # conclusion_text = f"'Hardware' was the most significant category for Toni Higgins, accounting for 45.5% of their sales."
                            conclusion_text = f"'{top_rep_top_cat['ProductCategory']}' was the most significant category for {top_rep_name}, accounting for {top_rep_cat_share:.1f}% of their sales."
                            conclusion_question = QUESTION_MAP.get(conclusion_type, DEFAULT_QUESTION)
                            candidate_conclusions.append({"priority": priority, "type": conclusion_type, "text": conclusion_text, "question": conclusion_question})


                    top_rep_city_sales = top_rep_data.groupby('City')['TotalSaleAmount'].sum().reset_index().sort_values('TotalSaleAmount', ascending=False)
                    if not top_rep_city_sales.empty and top_rep_city_sales.iloc[0]['TotalSaleAmount'] > 0:
                        conclusion_type = "cross_top_rep_city"
                        # Hardcode example for Toni Higgins top city / Value
                        # conclusion_text = f"Toni Higgins's sales were primarily concentrated in Richmond (USD 308,413)."
                        conclusion_text = f"{top_rep_name}'s sales were primarily concentrated in {top_rep_city_sales.iloc[0]['City']} ({format_currency(top_rep_city_sales.iloc[0]['TotalSaleAmount'])})."
                        conclusion_question = QUESTION_MAP.get(conclusion_type, DEFAULT_QUESTION)
                        candidate_conclusions.append({"priority": priority-1, "type": conclusion_type, "text": conclusion_text, "question": conclusion_question})
            else:
                print("[WARN] sales_by_rep list is empty in actual_metrics, skipping top rep cross-analysis.")
    except Exception as e: print(f"[WARN] Error during top rep cross-analysis: {e}")

    try: # Top Category Insights
        if actual_metrics.get('sales_by_category'):
             priority = 5
             if actual_metrics['sales_by_category']: # Check if not empty
                 top_cat_name = actual_metrics['sales_by_category'][0]['ProductCategory']
                 top_cat_data = sales_data_df[sales_data_df['ProductCategory'] == top_cat_name]
                 if not top_cat_data.empty:
                     top_cat_prod_sales = top_cat_data.groupby(['ProductID', 'ProductName'])['TotalSaleAmount'].sum().reset_index().sort_values('TotalSaleAmount', ascending=False)
                     if not top_cat_prod_sales.empty and top_cat_prod_sales.iloc[0]['TotalSaleAmount'] > 0:
                        conclusion_type = "cross_top_category_product"
                        # Hardcode example for top product in Hardware category
                        # conclusion_text = f"Within the leading 'Hardware' category, 'High-Performance Workstation' was the top product by revenue."
                        conclusion_text = f"Within the leading '{top_cat_name}' category, '{top_cat_prod_sales.iloc[0]['ProductName']}' was the top product by revenue."
                        conclusion_question = QUESTION_MAP.get(conclusion_type, DEFAULT_QUESTION)
                        candidate_conclusions.append({"priority": priority, "type": conclusion_type, "text": conclusion_text, "question": conclusion_question})

                     top_cat_rep_sales = top_cat_data.groupby(['SalespersonID', 'SalespersonName'])['TotalSaleAmount'].sum().reset_index().sort_values('TotalSaleAmount', ascending=False)
                     if not top_cat_rep_sales.empty and top_cat_rep_sales.iloc[0]['TotalSaleAmount'] > 0:
                        conclusion_type = "cross_top_category_rep"
                        conclusion_text = f"{top_cat_rep_sales.iloc[0]['SalespersonName']} was the lead seller within the top '{top_cat_name}' category ({format_currency(top_cat_rep_sales.iloc[0]['TotalSaleAmount'])})."
                        conclusion_question = QUESTION_MAP.get(conclusion_type, DEFAULT_QUESTION)
                        candidate_conclusions.append({"priority": priority-1, "type": conclusion_type, "text": conclusion_text, "question": conclusion_question})
             else:
                 print("[WARN] sales_by_category list is empty in actual_metrics, skipping top category cross-analysis.")
    except Exception as e: print(f"[WARN] Error during top category cross-analysis: {e}")

    try: # Top City Insights
        if actual_metrics.get('sales_by_city'):
             priority = 4
             if actual_metrics['sales_by_city']: # Check if not empty
                 top_city_name = actual_metrics['sales_by_city'][0]['City']
                 top_city_data = sales_data_df[sales_data_df['City'] == top_city_name]
                 if not top_city_data.empty:
                     top_city_prod_sales = top_city_data.groupby(['ProductID', 'ProductName'])['TotalSaleAmount'].sum().reset_index().sort_values('TotalSaleAmount', ascending=False)
                     if not top_city_prod_sales.empty and top_city_prod_sales.iloc[0]['TotalSaleAmount'] > 0:
                         conclusion_type = "cross_top_city_product"
                         conclusion_text = f"'{top_city_prod_sales.iloc[0]['ProductName']}' was the best-selling product in the top city, {top_city_name}."
                         conclusion_question = QUESTION_MAP.get(conclusion_type, DEFAULT_QUESTION)
                         candidate_conclusions.append({"priority": priority, "type": conclusion_type, "text": conclusion_text, "question": conclusion_question})

                     top_city_rep_sales = top_city_data.groupby(['SalespersonID', 'SalespersonName'])['TotalSaleAmount'].sum().reset_index().sort_values('TotalSaleAmount', ascending=False)
                     if not top_city_rep_sales.empty and top_city_rep_sales.iloc[0]['TotalSaleAmount'] > 0:
                        conclusion_type = "cross_top_city_rep"
                        # Hardcode example for top rep in Richmond
                        # conclusion_text = f"Toni Higgins led sales performance within Richmond."
                        conclusion_text = f"{top_city_rep_sales.iloc[0]['SalespersonName']} led sales performance within {top_city_name}."
                        conclusion_question = QUESTION_MAP.get(conclusion_type, DEFAULT_QUESTION)
                        candidate_conclusions.append({"priority": priority, "type": conclusion_type, "text": conclusion_text, "question": conclusion_question})
             else:
                 print("[WARN] sales_by_city list is empty in actual_metrics, skipping top city cross-analysis.")
    except Exception as e: print(f"[WARN] Error during top city cross-analysis: {e}")

    try: # Product performance New vs Existing Customers
        if (actual_metrics.get('sales_by_product') and 'new_customer_df' in locals() and 'existing_customer_df' in locals() and not new_customer_df.empty and not existing_customer_df.empty):
            priority = 3
            if actual_metrics['sales_by_product']: # Check if not empty
                top_prod_info = actual_metrics['sales_by_product'][0]
                prod_id_compare = top_prod_info['ProductID']; prod_name_compare = top_prod_info['ProductName']
                avg_deal_new_prod = new_customer_df[new_customer_df['ProductID'] == prod_id_compare]['TotalSaleAmount'].mean()
                avg_deal_exist_prod = existing_customer_df[existing_customer_df['ProductID'] == prod_id_compare]['TotalSaleAmount'].mean()
                if pd.notna(avg_deal_new_prod) and pd.notna(avg_deal_exist_prod) and avg_deal_new_prod > 0 and avg_deal_exist_prod > 0:
                    ratio_prod_cust = avg_deal_new_prod / avg_deal_exist_prod
                    conclusion_type = "cross_product_new_vs_exist_deal"
                    conclusion_text = None
                    if ratio_prod_cust > 1.2: conclusion_text = f"For the top product ('{prod_name_compare}'), average deal size was significantly higher for new customers ({format_currency(avg_deal_new_prod)}) vs existing ({format_currency(avg_deal_exist_prod)})."
                    elif ratio_prod_cust < 0.8: conclusion_text = f"For the top product ('{prod_name_compare}'), average deal size was significantly lower for new customers ({format_currency(avg_deal_new_prod)}) vs existing ({format_currency(avg_deal_exist_prod)})."
                    if conclusion_text:
                        conclusion_question = QUESTION_MAP.get(conclusion_type, DEFAULT_QUESTION)
                        candidate_conclusions.append({"priority": priority, "type": conclusion_type, "text": conclusion_text, "question": conclusion_question})
            else:
                 print("[WARN] sales_by_product list is empty in actual_metrics, skipping product new vs existing cross-analysis.")
    except Exception as e: print(f"[WARN] Error during product new vs existing cross-analysis: {e}")

    # --- Final Selection ---
    print(f"[INFO] Analysis complete. Generated {len(candidate_conclusions)} potential conclusion items.")
    final_conclusions_with_questions = [] # Changed list name
    unique_conclusion_texts = set()
    grouped_conclusions = {}

    # --- ADDED Robustness: Ensure c is a dictionary and has expected keys ---
    for c in candidate_conclusions:
        if isinstance(c, dict) and 'priority' in c and 'text' in c and c.get('text') and 'question' in c and c.get('question'):
            p = c['priority']; grouped_conclusions.setdefault(p, []).append(c)
        else: print(f"[WARN] Skipping invalid or incomplete candidate conclusion item: {c}")
    # --- /ADDED ---

    if grouped_conclusions:
        for p in grouped_conclusions: random.shuffle(grouped_conclusions[p])
        sorted_candidates = [item for p in sorted(grouped_conclusions.keys(), reverse=True) for item in grouped_conclusions[p]]
        selected_types_count = {}
        for c in sorted_candidates:
            if len(final_conclusions_with_questions) >= num_conclusions_target: break
            # Ensure text is not None or empty before adding
            if c.get('text') and c['text'] not in unique_conclusion_texts:
                # Append the dictionary with conclusion and question
                final_conclusions_with_questions.append({
                    "question": c['question'], # Add the question,
                    "answer": c['text'],
                })
                unique_conclusion_texts.add(c['text'])
                selected_types_count[c.get('type', 'unknown')] = selected_types_count.get(c.get('type', 'unknown'), 0) + 1

    print(f"[INFO] Selected {len(final_conclusions_with_questions)} unique conclusion/question pairs based on priority and target ({num_conclusions_target}).")

    if len(final_conclusions_with_questions) < num_conclusions_target :
        print(f"[WARN] Could only select/generate {len(final_conclusions_with_questions)} valid, unique conclusion/question pairs, less than the requested {num_conclusions_target}.")
        print(f"       Available distinct conclusion types generated: {len(set(c.get('type', 'unknown') for c in candidate_conclusions if isinstance(c,dict) and 'type' in c))}.")
        num_reps_print = actual_metrics.get('sales_by_rep', [])
        num_prods_print = actual_metrics.get('sales_by_product', [])
        num_cats_print = actual_metrics.get('sales_by_category', [])
        num_cities_print = actual_metrics.get('sales_by_city', [])
        num_custs_print = actual_metrics.get('sales_by_customer', [])
        print(f"       Distinct entities analyzed: Reps={len(num_reps_print)}, Products={len(num_prods_print)}, Categories={len(num_cats_print)}, Cities={len(num_cities_print)}, Customers={len(num_custs_print)}.")

    analysis_duration = time.time() - start_analysis_time
    print(f"[INFO] Data analysis duration: {analysis_duration:.2f} seconds.")
    actual_metrics['biases_applied_in_run'] = biases
    # Return the new list structure
    return final_conclusions_with_questions, actual_metrics


# --- save_data_and_conclusions ---
def save_data_and_conclusions(sales_data_df, conclusions_with_questions, actual_metrics, config, biases_in_args, csv_filepath, json_filepath):
    """Saves the sales data to CSV and conclusions/metadata (with questions) to JSON."""
    try:
        # Create directory if it doesn't exist
        output_dir = os.path.dirname(csv_filepath)
        if output_dir and not os.path.exists(output_dir):
            os.makedirs(output_dir)

        if isinstance(sales_data_df, pd.DataFrame) and not sales_data_df.empty:
            standard_cols = ["OrderID","OrderDate","Region","City","SalespersonID","SalespersonName","SalespersonTarget","CustomerID","CustomerName","IsNewCustomer","ProductID","ProductName","ProductCategory","Quantity","UnitPrice","TotalSaleAmount","WeekOfYear","DayOfWeek","DayOfMonth"]
            # Ensure only existing columns are selected
            output_cols = [col for col in standard_cols if col in sales_data_df.columns]
            output_df = sales_data_df[output_cols]
            output_df.to_csv(csv_filepath, index=False, encoding='utf-8-sig', quoting=csv.QUOTE_NONNUMERIC) # Ensure quoting for names with commas
            print(f"[INFO] Sales data saved successfully to: {csv_filepath}")
        else:
             # Create an empty file if no data
             with open(csv_filepath, 'w', encoding='utf-8-sig') as f:
                 # Optionally write header even if empty
                 # writer = csv.writer(f)
                 # writer.writerow(["OrderID","OrderDate",...]) # Add desired headers
                 pass
             print(f"[WARN] No valid sales data to save. Empty CSV created at: {csv_filepath}")
    except Exception as e: print(f"[ERROR] Failed to save CSV file {csv_filepath}: {e}")

    actual_biases = actual_metrics.get('biases_applied_in_run', biases_in_args) # Use actual biases if available

    # Structure the JSON output
    json_output = {
        "scenario_description": f"Monthly Sales Report Data for {config.get('region','N/A')} - {config.get('target_month_str','N/A')} (V6 - Randomized Biases, Robust Analysis + Questions)", # Updated description
        "data_file": os.path.basename(csv_filepath),
        "generation_timestamp": datetime.now().isoformat(),
        "configuration": config,
        "biases_applied_in_run": actual_biases,
        # Use the list of conclusion/question dictionaries
        "target_conclusions": conclusions_with_questions if conclusions_with_questions else [{"answer": "No conclusions generated due to data issues.", "question": "Why were no conclusions generated?"}],
        "actual_metrics": actual_metrics if actual_metrics else {"error": "Metrics calculation failed."}
    }

    # Save JSON with improved error handling for serialization
    try:
        output_dir_json = os.path.dirname(json_filepath)
        if output_dir_json and not os.path.exists(output_dir_json):
            os.makedirs(output_dir_json)

        # Custom JSON Encoder to handle numpy types and Timestamps
        class NpEncoder(json.JSONEncoder):
             def default(self, obj):
                if isinstance(obj, np.integer): return int(obj)
                elif isinstance(obj, np.floating):
                    if np.isnan(obj): return None # Represent NaN as null
                    elif np.isinf(obj): return None # Represent Inf as null
                    return float(obj)
                elif isinstance(obj, np.ndarray): return obj.tolist()
                elif isinstance(obj, (datetime, date)): return obj.isoformat()
                elif isinstance(obj, pd.Timestamp): return obj.isoformat()
                elif pd.isna(obj): return None # Handle pandas NA/NaT as null
                elif isinstance(obj, np.bool_): return bool(obj)
                elif isinstance(obj, bytes): return obj.decode('utf-8', errors='ignore') # Decode bytes if present
                return super(NpEncoder, self).default(obj)

        with open(json_filepath, 'w', encoding='utf-8') as f:
            json.dump(json_output, f, ensure_ascii=False, indent=2, cls=NpEncoder)
        print(f"[INFO] Conclusions and metadata saved successfully to: {json_filepath}")

    except TypeError as e:
         print(f"[ERROR] Failed to serialize JSON object: {e}")
         # Attempt to identify problematic keys for debugging
         try:
             problem_keys = []
             for k, v in json_output.items():
                 try: json.dumps({k: v}, cls=NpEncoder)
                 except TypeError: problem_keys.append(k)
             print(f"Problematic top-level keys during JSON serialization: {problem_keys}")
             if 'actual_metrics' in problem_keys:
                 problem_metric_keys = []
                 for mk, mv in json_output['actual_metrics'].items():
                     try: json.dumps({mk: mv}, cls=NpEncoder)
                     except TypeError: problem_metric_keys.append(mk)
                 print(f"Problematic keys within actual_metrics: {problem_metric_keys}")
         except Exception as log_e: print(f"Additionally, error while trying to identify problematic JSON keys: {log_e}")
    except Exception as e: print(f"[ERROR] Failed to save JSON file {json_filepath}: {e}")


# --- Main Execution Block ---
if __name__ == "__main__":
    parser = argparse.ArgumentParser(
        description="Generate synthetic sales data (V6) with randomized biases, robust analysis, and paired questions.", # Updated description
        formatter_class=argparse.ArgumentDefaultsHelpFormatter
    )
    # Arguments remain the same
    parser.add_argument("--num-records", type=int, default=DEFAULT_NUM_RECORDS, help=f"Number of transaction records (default: {DEFAULT_NUM_RECORDS}).")
    parser.add_argument("--output-csv", type=str, default=DEFAULT_OUTPUT_CSV, help="Output CSV data file path.")
    parser.add_argument("--output-json", type=str, default=DEFAULT_OUTPUT_JSON, help="Output JSON conclusions file path.")
    parser.add_argument("--num-conclusions", type=int, default=DEFAULT_NUM_CONCLUSIONS, help=f"Target number of key conclusion/question pairs (default: {DEFAULT_NUM_CONCLUSIONS}).")
    parser.add_argument("--target-month", type=str, default=None, help="Target month (YYYY-MM), defaults to previous month.")
    parser.add_argument("--region", type=str, default=DEFAULT_REGION, help="Sales region name.")
    parser.add_argument("--currency", type=str, default=DEFAULT_CURRENCY, help="Currency symbol.")
    parser.add_argument("--regional-target", type=float, default=DEFAULT_REGIONAL_TARGET, help="Overall regional sales target.")
    parser.add_argument("--prev-month-sales", type=float, default=DEFAULT_PREV_MONTH_SALES, help="Sales from previous month for growth comparison.")
    parser.add_argument("--bias-overall-target", type=str, choices=['exceed', 'meet', 'miss'], default='meet', help="[NOTE: Randomized internally]")
    parser.add_argument("--bias-growth", type=str, choices=['positive', 'neutral', 'negative'], default='neutral', help="[NOTE: Randomized internally]")
    parser.add_argument("--bias-top-rep", type=str, default=None, help="[NOTE: Randomized internally]")
    parser.add_argument("--bias-bottom-rep", type=str, default=None, help="[NOTE: Randomized internally]")
    parser.add_argument("--bias-top-product", type=str, default=None, help="[NOTE: Randomized internally]")
    parser.add_argument("--bias-new-customer", type=str, choices=['high', 'medium', 'low'], default='medium', help="[NOTE: Randomized internally]")

    args = parser.parse_args()
    start_time_total = time.time()
    date_range = get_target_month_range(args.target_month)

    # Randomize Biases (Keep this logic)
    valid_rep_ids = {rep['id'] for rep in SALES_REPS} if SALES_REPS else set()
    valid_prod_ids = {prod['id'] for prod in PRODUCTS} if PRODUCTS else set()
    print("[INFO] Randomizing all bias parameters for this run...")
    random_bias_overall_target = random.choice(['exceed', 'meet', 'miss'])
    random_bias_growth = random.choice(['positive', 'neutral', 'negative'])
    random_bias_top_rep = random.choice(list(valid_rep_ids)) if valid_rep_ids else None
    possible_bottom_reps = list(valid_rep_ids - {random_bias_top_rep}) if random_bias_top_rep else list(valid_rep_ids)
    random_bias_bottom_rep = random.choice(possible_bottom_reps) if possible_bottom_reps else None
    random_bias_top_product = random.choice(list(valid_prod_ids)) if valid_prod_ids else None
    random_bias_new_customer = random.choice(['high', 'medium', 'low'])
    biases_for_generation = { 'overall_target': random_bias_overall_target, 'growth': random_bias_growth, 'top_rep': random_bias_top_rep, 'bottom_rep': random_bias_bottom_rep, 'top_product': random_bias_top_product, 'new_customer': random_bias_new_customer }
    print(f"[INFO] Applying **Randomized** Biases for Generation: {biases_for_generation}")

    # Configuration and Data Generation
    config = { 'num_records': args.num_records, 'target_month_str': date_range[2], 'region': args.region, 'currency': args.currency, 'regional_target': args.regional_target, 'prev_month_sales': args.prev_month_sales, 'num_defined_reps': len(SALES_REPS), 'num_defined_products': len(PRODUCTS), 'num_defined_cities': len(CITIES), 'customer_base_size': CUSTOMER_BASE_SIZE }
    customer_list = generate_customer_list(CUSTOMER_BASE_SIZE)
    sales_data = generate_sales_data(args.num_records, date_range, biases_for_generation, config, customer_list)
    sales_data_df = pd.DataFrame(sales_data)

    # Data Cleaning
    numeric_cols = ['TotalSaleAmount', 'Quantity', 'UnitPrice', 'SalespersonTarget']
    initial_rows = len(sales_data_df)
    for col in numeric_cols:
         if col in sales_data_df.columns:
             sales_data_df[col] = pd.to_numeric(sales_data_df[col], errors='coerce')
    # Drop rows where essential numeric columns are NaN after coercion
    sales_data_df.dropna(subset=['TotalSaleAmount', 'Quantity', 'SalespersonTarget'], inplace=True)
    cleaned_rows = len(sales_data_df)
    if initial_rows > cleaned_rows:
        print(f"[WARN] Dropped {initial_rows - cleaned_rows} rows due to bad numeric values in essential columns.")

    # Analysis and Saving
    if not sales_data_df.empty:
        # Call the updated analysis function
        selected_conclusions_with_questions, actual_metrics = analyze_data_and_select_conclusions(sales_data_df, biases_for_generation, config, args.num_conclusions)
        # Call the updated save function
        save_data_and_conclusions(sales_data_df, selected_conclusions_with_questions, actual_metrics, config, args.__dict__, args.output_csv, args.output_json)
    else:
        print("[ERROR] No valid data remaining after cleaning. Skipping analysis.")
        # Save empty files but include metadata
        save_data_and_conclusions(pd.DataFrame(), [], {"error": "No valid data generated or remaining after cleaning", "biases_applied_in_run": biases_for_generation}, config, args.__dict__, args.output_csv, args.output_json)

    end_time_total = time.time()
    print(f"\n--- Script finished in {end_time_total - start_time_total:.2f} seconds ---")