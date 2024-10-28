import shap
import pandas as pd
from openpyxl import Workbook

def generate_shap_excel(model, X, excel_filename='shap_results.xlsx'):
    # Initialize SHAP explainer
    explainer = shap.TreeExplainer(model)
    shap_values = explainer.shap_values(X)  # Get SHAP values for all rows

    # Create an Excel workbook and sheet
    wb = Workbook()
    ws = wb.active
    ws.title = "SHAP Analysis"

    # Add header to the Excel sheet
    headers = list(X.columns) + ['Prediction', 'Top 3 Features', 'Lowest 3 Features']
    ws.append(headers)

    # Loop over each row in the dataset
    for idx, row in X.iterrows():
        # Convert the row to a list of values
        row_data = row.tolist()

        # Get SHAP values for the current row
        shap_row_values = shap_values[1][idx]  # For class 1 predictions

        # Extract top 3 and lowest 3 feature contributions
        sorted_contributions = sorted(
            zip(X.columns, shap_row_values), key=lambda x: abs(x[1]), reverse=True
        )
        top_3 = ', '.join([f"{feat}: {round(val, 2)}" for feat, val in sorted_contributions[:3]])
        lowest_3 = ', '.join([f"{feat}: {round(val, 2)}" for feat, val in sorted_contributions[-3:]])

        # Predict the outcome for the row
        prediction = model.predict([row])[0]

        # Prepare the data for the current row in Excel
        excel_row = row_data + [prediction, top_3, lowest_3]

        # Append the row data to the Excel sheet
        ws.append(excel_row)

    # Save the Excel file
    wb.save(excel_filename)
    print(f"Excel file saved: {excel_filename}")

# Example usage:
# Assuming 'rf_model' is your trained RandomForest model and 'X_test' is your feature dataset
# generate_shap_excel(rf_model, X_test)
