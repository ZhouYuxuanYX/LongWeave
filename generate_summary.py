import os
import json
import pandas as pd
import yaml
from collections import OrderedDict

# --- Configuration ---
BASE_RESULTS_DIR = './results'
CONFIG_FILE_PATH = './config/global_config.yaml'

# Output filenamesPp
OUTPUT_SUMMARY_CSV_FILE = 'longeval_summary_report.csv'
OUTPUT_SUMMARY_EXCEL_FILE = 'longeval_summary_report.xlsx'
OUTPUT_DETAILED_CSV_FILE = 'longeval_detailed_report.csv'
OUTPUT_DETAILED_EXCEL_FILE = 'longeval_detailed_report.xlsx'

# Define the primary metric to display for each task in the SUMMARY report
PRIMARY_METRICS = {
    "CODE_FIXING": "total_score",
    "GEN_KV_DICT": "total_score",
    "KG_TO_TEXT": "sentence_coverage_rate",
    "AP_STYLE_WRITING": "total_score",
    "PARAGRAPH_ORDERING": "kendalls_tau",
    "SALES_REPORT_GENERATION": "total_score",
    "STATE_MACHINE": "match_ratio"
}

MODEL_ORDER = []
# --- End Configuration ---

PER_TASK_AVG_IDENTIFIER = "Avg" # Used for average of a task across its lengths, or average of 'Overall' across its lengths
GRAND_OVERALL_TASK_IDENTIFIER = "Overall" # Task name for averages across multiple tasks


def load_order_from_config(config_path):
    global MODEL_ORDER 

    try:
        with open(config_path, 'r') as f:
            config_data = yaml.safe_load(f)
    except FileNotFoundError:
        print(f"Error: Config file not found at {config_path}")
        return None, None
    except yaml.YAMLError as e:
        print(f"Error parsing YAML file {config_path}: {e}")
        return None, None
    except Exception as e:
        print(f"Error reading config file {config_path}: {e}")
        return None, None

    if not config_data:
        print(f"Error: Config data is empty in {config_path}")
        return None, None

    MODEL_ORDER = config_data.get('model_order', [])
    if MODEL_ORDER:
        print(f"Loaded Model Order from Config: {MODEL_ORDER}")
    else:
        print("No 'model_order' key found in config or it was empty. Models will be sorted alphabetically by default.")

    if 'selected_tasks' not in config_data:
        print(f"Error: 'selected_tasks' key not found in {config_path}")
        return None, None

    task_order_dict = OrderedDict()
    length_order_dict = OrderedDict() # To store unique lengths in order of appearance

    for task_entry in config_data.get('selected_tasks', []):
        if 'task_path' in task_entry:
            parts = task_entry['task_path'].split('/')
            if len(parts) >= 1:
                base_task = parts[0]
                if base_task not in task_order_dict:
                    task_order_dict[base_task] = None
                if len(parts) == 2:
                    length = parts[1]
                    if length not in length_order_dict: # Only add unique lengths
                        length_order_dict[length] = None # Value doesn't matter, only keys for order
            else:
                print(f"Warning: Skipping invalid task_path format in config: {task_entry['task_path']}")

    task_order = list(task_order_dict.keys())
    length_order = list(length_order_dict.keys()) # These are specific lengths like '1k', '2k'

    if not task_order:
        print("Error: Could not extract valid task order from config.")
        return None, length_order # length_order might still be populated

    print(f"Loaded Task Order from Config: {task_order}")
    print(f"Loaded Length Order from Config: {length_order}")
    return task_order, length_order


def parse_task_key(task_key):
    parts = task_key.split('/')
    if len(parts) == 2:
        return parts[0], parts[1] # e.g., ("CODE_FIXING", "1k") or ("CODE_FIXING", "Avg")
    elif len(parts) == 1:
        # This case might occur if a JSON key is just "TASK_NAME" representing an overall average for that task.
        # The current logic in process_all_results expects "TASK_NAME/Avg" for pre-calculated averages.
        # If "TASK_NAME" itself is a key with scores, this interprets it as (TASK_NAME, PER_TASK_AVG_IDENTIFIER).
        print(f"Warning: Encountered single-part task key: {task_key}. Interpreting its length component as '{PER_TASK_AVG_IDENTIFIER}'. Ensure this is intended.")
        return parts[0], PER_TASK_AVG_IDENTIFIER
    else:
        print(f"Warning: Unexpected task key format: {task_key}")
        return task_key, None


def process_all_results(base_dir, valid_tasks_config, valid_lengths_config): # valid_lengths_config contains specific lengths like '1k', '2k'
    if not os.path.isdir(base_dir):
        print(f"Error: Base directory not found: {base_dir}")
        return None, None

    processed_data = []
    all_metrics_found = set()

    model_dirs = [d for d in os.listdir(base_dir) if os.path.isdir(os.path.join(base_dir, d))]
    print(f"\nFound {len(model_dirs)} potential model directories: {model_dirs}")

    for model_name in model_dirs:
        print(f"Processing model: {model_name}...")
        json_file_name = f"{model_name}_metric_report.json"
        json_file_path = os.path.join(base_dir, model_name, json_file_name)

        if not os.path.exists(json_file_path):
            print(f"  Skipping: Report file not found at {json_file_path}")
            continue

        try:
            with open(json_file_path, 'r') as f:
                report_data = json.load(f)
        except Exception as e:
            print(f"  Error reading or parsing {json_file_path}: {e}")
            continue

        for task_key, task_metrics_data in report_data.items():
            base_task, length_identifier_from_key = parse_task_key(task_key)

            if base_task not in valid_tasks_config:
                # print(f"  Skipping task: {base_task} (from key {task_key}) as it's not in valid_tasks_config.")
                continue

            # Check if the length_identifier is a specific configured length OR 
            # if it's the special PER_TASK_AVG_IDENTIFIER coming from a "Task/Avg" JSON key.
            is_specific_length = length_identifier_from_key in valid_lengths_config
            is_per_task_average_format_from_json = (length_identifier_from_key == PER_TASK_AVG_IDENTIFIER and
                                                    task_key == f"{base_task}/{PER_TASK_AVG_IDENTIFIER}")

            if not (is_specific_length or is_per_task_average_format_from_json):
                # print(f"  Skipping entry: {task_key} - length_id '{length_identifier_from_key}' is not a configured specific length nor a valid pre-calculated per-task avg format.")
                continue
            
            actual_length_for_df = length_identifier_from_key # This will be '1k', '2k', or 'Avg' (if it's a per-task average from JSON)

            for metric_name, metric_details in task_metrics_data.items():
                all_metrics_found.add(metric_name)
                if isinstance(metric_details, dict) and 'average' in metric_details:
                    score = metric_details['average']
                    processed_data.append({
                        'Model': model_name,
                        'Task': base_task,
                        'Length': actual_length_for_df, 
                        'Metric': metric_name,
                        'Score': score
                    })
    
    sorted_all_metrics_found = sorted(list(all_metrics_found))
    print(f"All unique metrics found across reports: {sorted_all_metrics_found}")
    return processed_data, sorted_all_metrics_found

# --- Main Execution ---
if __name__ == "__main__":
    print(f"Loading configuration from: {CONFIG_FILE_PATH}")
    TASK_ORDER_CONFIG, LENGTH_ORDER_CONFIG = load_order_from_config(CONFIG_FILE_PATH) # LENGTH_ORDER_CONFIG contains specific lengths like '1k', '2k'

    if TASK_ORDER_CONFIG is None:
        print("Exiting due to configuration loading error (Tasks).")
        exit()
    
    # safe_length_order_config will be the list of specific lengths (e.g., ['1k', '2k'])
    # It's "safe" because it handles if LENGTH_ORDER_CONFIG is None.
    safe_length_order_config = LENGTH_ORDER_CONFIG if LENGTH_ORDER_CONFIG is not None else []
    if not safe_length_order_config:
        print("Warning: No specific lengths defined in LENGTH_ORDER_CONFIG from config file. Some summary averages might not be calculable or meaningful.")


    print(f"\nStarting report generation from: {BASE_RESULTS_DIR}")
    all_flat_data, ALL_METRICS_ORDER = process_all_results(BASE_RESULTS_DIR, TASK_ORDER_CONFIG, safe_length_order_config)

    if not all_flat_data:
        print("No data processed based on the configuration and available reports. Exiting.")
        exit()

    df_all_flat = pd.DataFrame(all_flat_data)
    print(f"\nProcessed {len(df_all_flat)} total data points (all metrics).")

    # --- 1. Generate Summary Report (Primary Metrics Only) ---
    print("\n--- Generating Summary Report ---")
    summary_data = []
    for _, row in df_all_flat.iterrows():
        primary_metric = PRIMARY_METRICS.get(row['Task'])
        # We only want rows where Metric is the primary one for that Task.
        # The row['Length'] can be a specific length (e.g., '1k') or PER_TASK_AVG_IDENTIFIER 
        # (if it came from a "Task/Avg" entry in the JSON).
        if primary_metric and row['Metric'] == primary_metric:
            if row['Length'] in safe_length_order_config or row['Length'] == PER_TASK_AVG_IDENTIFIER:
                 summary_data.append(row)

    if not summary_data:
        print("No data found matching primary metrics for the summary report (after filtering for relevant lengths/averages).")
    else:
        df_summary_flat = pd.DataFrame(summary_data)
        print(f"Filtered down to {len(df_summary_flat)} data points for summary report (primary metrics, specific lengths, or pre-calculated Task/Avg).")
        
        try:
            summary_pivot_df = pd.pivot_table(df_summary_flat,
                                              values='Score',
                                              index='Model',
                                              columns=['Task', 'Length'], # Length here can be '1k', '2k', ..., or 'Avg' (if from JSON)
                                              aggfunc='first') # Use 'first' as we expect unique (Model, Task, Length, PrimaryMetric)
            print("Initial summary pivot table created.")

            # Step 1: Calculate/Overwrite Per-Task Average (across its specific configured lengths)
            # This ensures (Task, PER_TASK_AVG_IDENTIFIER) columns are consistently calculated
            # based on the primary metrics of specific lengths for that task.
            print("Calculating/Updating per-task averages (e.g., TaskX-Avg)...")
            for task_name_iter in TASK_ORDER_CONFIG: # Iterate through configured tasks
                cols_for_this_task_avg = []
                # Collect scores only from specific, configured lengths for this task
                for len_val in safe_length_order_config: 
                    column_tuple = (task_name_iter, len_val)
                    if column_tuple in summary_pivot_df.columns:
                        cols_for_this_task_avg.append(column_tuple)
                
                if cols_for_this_task_avg:
                    # Calculate the average and add/overwrite it in the pivot table
                    avg_col_tuple = (task_name_iter, PER_TASK_AVG_IDENTIFIER)
                    summary_pivot_df[avg_col_tuple] = summary_pivot_df[cols_for_this_task_avg].mean(axis=1, skipna=True)
                # else:
                    # print(f"  - No specific length data found for task '{task_name_iter}' to calculate its average.")
            print("Completed calculation/update of per-task averages.")

            # Step 2: Calculate Overall Average Score for each specific CONFIGURED LENGTH (across all tasks)
            print(f"Calculating per-length overall averages (e.g., {GRAND_OVERALL_TASK_IDENTIFIER}-1k)...")
            for length_val in safe_length_order_config: # Iterate through '1k', '2k', etc.
                cols_for_this_length_avg = []
                for task_item in TASK_ORDER_CONFIG:
                    if (task_item, length_val) in summary_pivot_df.columns: # Only use scores for this specific length
                        cols_for_this_length_avg.append((task_item, length_val))
                
                if cols_for_this_length_avg:
                    summary_pivot_df[(GRAND_OVERALL_TASK_IDENTIFIER, length_val)] = summary_pivot_df[cols_for_this_length_avg].mean(axis=1, skipna=True)
            print(f"Completed calculation of per-length overall averages.")

            # Step 3: Calculate Grand Overall Average (across all tasks and all specific configured lengths)
            print(f"Calculating grand overall average ('{GRAND_OVERALL_TASK_IDENTIFIER}', '{PER_TASK_AVG_IDENTIFIER}')...")
            cols_for_grand_avg_calc = []
            for task_item in TASK_ORDER_CONFIG:
                for length_item in safe_length_order_config: # Only average over specific lengths
                    if (task_item, length_item) in summary_pivot_df.columns:
                        cols_for_grand_avg_calc.append((task_item, length_item))

            if cols_for_grand_avg_calc:
                summary_pivot_df[(GRAND_OVERALL_TASK_IDENTIFIER, PER_TASK_AVG_IDENTIFIER)] = summary_pivot_df[cols_for_grand_avg_calc].mean(axis=1, skipna=True)
            print(f"Completed calculation of grand overall average.")


            # Step 4: Define Final Column Order for Summary Report
            final_summary_columns = []
            # Part A: Task-specific columns (Task-Length1, Task-Length2, ..., Task-Avg)
            for task in TASK_ORDER_CONFIG:
                for length in safe_length_order_config: 
                    if (task, length) in summary_pivot_df.columns:
                        final_summary_columns.append((task, length))
                # Add the (Task, PER_TASK_AVG_IDENTIFIER) column, which was calculated/updated in Step 1
                if (task, PER_TASK_AVG_IDENTIFIER) in summary_pivot_df.columns: 
                    final_summary_columns.append((task, PER_TASK_AVG_IDENTIFIER))
            
            # Part B: Overall-per-Length averages (Overall-Length1, Overall-Length2, ...)
            for length in safe_length_order_config:
                overall_per_length_col_name = (GRAND_OVERALL_TASK_IDENTIFIER, length)
                if overall_per_length_col_name in summary_pivot_df.columns:
                    final_summary_columns.append(overall_per_length_col_name)

            # Part C: Grand Overall Average (Overall-Avg)
            grand_avg_col_name = (GRAND_OVERALL_TASK_IDENTIFIER, PER_TASK_AVG_IDENTIFIER)
            if grand_avg_col_name in summary_pivot_df.columns:
                final_summary_columns.append(grand_avg_col_name)
            
            if final_summary_columns:
                # Ensure all columns in final_summary_columns exist in summary_pivot_df before reindexing
                existing_final_columns = [col for col in final_summary_columns if col in summary_pivot_df.columns]
                if len(existing_final_columns) != len(final_summary_columns):
                    print(f"Warning: Some defined final summary columns were not found in the pivot table. Using only existing columns for reordering.")
                
                if existing_final_columns: # Reindex only if there are valid columns to reindex with
                    summary_pivot_df = summary_pivot_df.reindex(columns=pd.MultiIndex.from_tuples(existing_final_columns, names=['Task', 'Length']))
                    print("Summary columns reordered.")
                else:
                    print("Warning: No valid columns for final summary order after checking existence. Skipping column reordering.")
            else:
                print("Warning: No columns identified for final summary order. Skipping column reordering.")

            if MODEL_ORDER:
                actual_models = summary_pivot_df.index.tolist()
                ordered_models_from_config = [m for m in MODEL_ORDER if m in actual_models]
                remaining_models = sorted([m for m in actual_models if m not in MODEL_ORDER])
                final_model_order = ordered_models_from_config + remaining_models
                summary_pivot_df = summary_pivot_df.reindex(index=final_model_order)
                print("Summary rows (models) reordered based on MODEL_ORDER and then alphabetically.")
            else:
                summary_pivot_df.sort_index(inplace=True)
                print("Summary rows (models) sorted alphabetically.")

            summary_pivot_df = summary_pivot_df.round(4)

            try:
                summary_pivot_df.to_excel(OUTPUT_SUMMARY_EXCEL_FILE, merge_cells=True)
                print(f"Successfully saved SUMMARY report to Excel: {OUTPUT_SUMMARY_EXCEL_FILE}")
            except Exception as e:
                print(f"Error saving SUMMARY report to Excel ({OUTPUT_SUMMARY_EXCEL_FILE}): {e}")
            try:
                summary_pivot_df.to_csv(OUTPUT_SUMMARY_CSV_FILE)
                print(f"Successfully saved SUMMARY report to CSV: {OUTPUT_SUMMARY_CSV_FILE}")
            except Exception as e_csv:
                print(f"Error saving SUMMARY report to CSV ({OUTPUT_SUMMARY_CSV_FILE}): {e_csv}")
        except Exception as e:
            print(f"\nAn error occurred during SUMMARY report generation: {e}")
            if 'df_summary_flat' in locals() and not df_summary_flat.empty:
                 print("--- Debug df_summary_flat (first 5 rows): ---")
                 print(df_summary_flat.head())
            if 'summary_pivot_df' in locals() and not summary_pivot_df.empty:
                 print("--- Debug summary_pivot_df (first 5 rows, first 5 columns): ---")
                 print(summary_pivot_df.iloc[:5, :min(5, summary_pivot_df.shape[1])])


    # --- 2. Generate Detailed Report (All Metrics with new hierarchy) ---
    print("\n--- Generating Detailed Report ---")
    if df_all_flat.empty:
         print("Cannot generate detailed report, no data was processed.")
    else:
        try:
            detailed_pivot_df = pd.pivot_table(df_all_flat,
                                               values='Score',
                                               index='Model',
                                               columns=['Task', 'Metric', 'Length'], # Length can be '1k'...'Avg' from JSON
                                               aggfunc='first')
            print("Detailed pivot table created with new hierarchy.")

            final_detailed_columns = []
            # For detailed report, length order should include specific configured lengths and the PER_TASK_AVG_IDENTIFIER (if present from JSON)
            ordered_lengths_for_detailed = safe_length_order_config + [PER_TASK_AVG_IDENTIFIER]


            for task in TASK_ORDER_CONFIG:
                for metric in ALL_METRICS_ORDER: # ALL_METRICS_ORDER is sorted list of all unique metric names found
                    for length in ordered_lengths_for_detailed: # Use specific lengths AND 'Avg' (if it came from JSON as Task/Avg)
                        col_tuple = (task, metric, length)
                        if col_tuple in detailed_pivot_df.columns:
                             final_detailed_columns.append(col_tuple)

            if not final_detailed_columns:
                print("Warning: No columns were matched for the detailed report's final order. The table might be empty or incorrectly structured.")
            else:
                print(f"Attempting to reindex detailed_pivot_df with {len(final_detailed_columns)} columns.")
                # Ensure all columns in final_detailed_columns exist before reindexing
                existing_final_detailed_cols = [col for col in final_detailed_columns if col in detailed_pivot_df.columns]
                if len(existing_final_detailed_cols) != len(final_detailed_columns):
                     print(f"Warning: Some defined final detailed columns were not found in the pivot table. Using only existing columns for reordering.")

                if existing_final_detailed_cols:
                    detailed_pivot_df = detailed_pivot_df.reindex(columns=pd.MultiIndex.from_tuples(existing_final_detailed_cols, names=['Task', 'Metric', 'Length']))
                    print("Detailed columns reordered based on Task -> Metric -> Length.")
                else:
                    print("Warning: No valid columns for final detailed order after checking existence. Skipping column reordering.")


            if MODEL_ORDER:
                actual_models = detailed_pivot_df.index.tolist()
                ordered_models_from_config = [m for m in MODEL_ORDER if m in actual_models]
                remaining_models = sorted([m for m in actual_models if m not in MODEL_ORDER])
                final_model_order = ordered_models_from_config + remaining_models
                detailed_pivot_df = detailed_pivot_df.reindex(index=final_model_order)
                print("Detailed rows (models) reordered based on MODEL_ORDER and then alphabetically.")
            else:
                 detailed_pivot_df.sort_index(inplace=True)
                 print("Detailed rows (models) sorted alphabetically.")

            detailed_pivot_df = detailed_pivot_df.round(4)

            try:
                detailed_pivot_df.to_excel(OUTPUT_DETAILED_EXCEL_FILE, merge_cells=True)
                print(f"Successfully saved DETAILED report to Excel: {OUTPUT_DETAILED_EXCEL_FILE}")
            except Exception as e:
                print(f"Error saving DETAILED report to Excel ({OUTPUT_DETAILED_EXCEL_FILE}): {e}")
            try:
                detailed_pivot_df.to_csv(OUTPUT_DETAILED_CSV_FILE)
                print(f"Successfully saved DETAILED report to CSV: {OUTPUT_DETAILED_CSV_FILE}")
            except Exception as e_csv:
                print(f"Error saving DETAILED report to CSV ({OUTPUT_DETAILED_CSV_FILE}): {e_csv}")

        except Exception as e:
            print(f"\nAn error occurred during DETAILED report generation: {e}")
            if 'df_all_flat' in locals() and not df_all_flat.empty:
                print("--- Debug: Check the structure of df_all_flat ---")
                print(f"Shape of all flat data: {df_all_flat.shape}")
            if 'detailed_pivot_df' in locals() and not detailed_pivot_df.empty:
                 print(f"Shape of detailed pivot table: {detailed_pivot_df.shape}")
                 print("First 5 columns of detailed_pivot_df.columns:")
                 print(detailed_pivot_df.columns[:min(5, detailed_pivot_df.shape[1])])
            print("--------------------------------------------------")

    print("\nReport generation finished.")