import pandas as pd
import plotly.graph_objects as go
import io

df = pd.read_csv("./processed_transactions.csv")

# Calculate total cash_in, cash_out, and cash_investments
total_cash_in = df[df['cash_flow'] == 'cash_in']['amount'].sum()

# Calculate absolute values for cash_out and cash_investments amounts
df['amount_abs'] = df['amount'].abs()

total_cash_out = df[df['cash_flow'] == 'cash_out']['amount_abs'].sum()
total_cash_investments = df[df['cash_flow'] == 'cash_investments']['amount_abs'].sum()

# Calculate cash_out and cash_investments breakdown by vendor
cash_out_by_vendor = df[df['cash_flow'] == 'cash_out'].groupby('vendors')['amount_abs'].sum().reset_index()
cash_investments_by_vendor = df[df['cash_flow'] == 'cash_investments'].groupby('vendors')['amount_abs'].sum().reset_index()

# Create Sankey chart nodes and links
# Define all unique labels for the nodes
labels = ['Cash_in', 'Cash_out', 'Cash_investments'] + \
         cash_out_by_vendor['vendors'].tolist() + \
         cash_investments_by_vendor['vendors'].tolist()

# Create a mapping from label to an integer index for the Sankey chart
label_map = {label: i for i, label in enumerate(labels)}

# Initialize lists for Sankey link source, target, and value
source = []
target = []
value = []

# Create links from 'Cash_in' to 'Cash_out' and 'Cash_investments'
source.append(label_map['Cash_in'])
target.append(label_map['Cash_out'])
value.append(total_cash_out)

source.append(label_map['Cash_in'])
target.append(label_map['Cash_investments'])
value.append(total_cash_investments)

# Create links from 'Cash_out' to individual vendors
for index, row in cash_out_by_vendor.iterrows():
    source.append(label_map['Cash_out'])
    target.append(label_map[row['vendors']])
    value.append(row['amount_abs'])

# Create links from 'Cash_investments' to individual vendors
for index, row in cash_investments_by_vendor.iterrows():
    source.append(label_map['Cash_investments'])
    target.append(label_map[row['vendors']])
    value.append(row['amount_abs'])

# Create the Sankey chart figure
fig = go.Figure(data=[go.Sankey(
    node=dict(
        pad=15,       # Padding between nodes
        thickness=20, # Thickness of the nodes
        line=dict(color='black', width=0.5), # Node border
        label=labels  # Labels for the nodes
    ),
    link=dict(
        source=source, # Indices of the source nodes
        target=target, # Indices of the target nodes
        value=value    # Values of the links
    ))])

# Update the layout with a title and font size
fig.update_layout(title_text="Cash Flow Sankey Diagram", font_size=10)

# Save the chart as an HTML file in the current directory
fig.write_html("cash_flow_sankey_chart.html")
