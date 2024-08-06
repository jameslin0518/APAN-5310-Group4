import pandas as pd
import numpy as np

# Generate Stores and Store Expenses data with New York addresses
ny_addresses = [
    "123 Main St, New York, NY, 10001",
    "456 Elm St, New York, NY, 10002",
    "789 Oak St, New York, NY, 10003",
    "101 Maple St, New York, NY, 10004",
    "202 Pine St, New York, NY, 10005"
]

# Updated data for Stores and Store Expenses
stores_data_ny = {
    "StoreID": [1, 2, 3, 4, 5],
    "StoreName": [
        "ABC Foodmart - Downtown",
        "ABC Foodmart - Northside",
        "ABC Foodmart - Eastside",
        "ABC Foodmart - Westside",
        "ABC Foodmart - Southside"
    ],
    "Location": ny_addresses,
    "ManagerID": [101, 102, 103, 104, 105],
    "Utility": [2000.00, 1800.00, 2200.00, 1900.00, 2100.00],
    "Marketing": [1500.00, 1300.00, 1600.00, 1400.00, 1550.00],
    "Security": [500.00, 450.00, 550.00, 480.00, 520.00],
    "Supplies": [800.00, 750.00, 850.00, 780.00, 820.00],
    "Maintenance": [1200.00, 1100.00, 1300.00, 1150.00, 1250.00],
    "OtherExpense": [300.00, 250.00, 350.00, 280.00, 320.00]
}

# Creating DataFrame
stores_df_ny = pd.DataFrame(stores_data_ny)

# Saving to Excel
file_path_ny_stores = "Stores_and_StoreExpenses_NY.xlsx"
stores_df_ny.to_excel(file_path_ny_stores, index=False)
# Generate Employees and Work Hours data for New York stores
# Creating sample data for Employees with StoreID 1-5 only
employees_data_ny = {
    "EmployeeID": range(1, 21),
    "FirstName": [
        "John", "Jane", "Jim", "Jill", "Jack",
        "Jenny", "Joe", "Julie", "James", "Janet",
        "Jeff", "Jasmine", "Jacob", "Jessica", "Jerry",
        "Jocelyn", "Jordan", "Joy", "Jason", "Joan"
    ],
    "LastName": [
        "Doe", "Smith", "Brown", "Johnson", "Williams",
        "Jones", "Garcia", "Miller", "Davis", "Rodriguez",
        "Martinez", "Hernandez", "Lopez", "Gonzalez", "Wilson",
        "Anderson", "Thomas", "Taylor", "Moore", "Jackson"
    ],
    "JobTitle": [
        "Cashier", "Manager", "Stock Clerk", "Customer Service", "Cashier",
        "Manager", "Stock Clerk", "Customer Service", "Cashier", "Manager",
        "Stock Clerk", "Customer Service", "Cashier", "Manager", "Stock Clerk",
        "Customer Service", "Cashier", "Manager", "Stock Clerk", "Customer Service"
    ],
    "StoreID": np.random.choice(range(1, 6), 20),
    "Salary": np.random.uniform(30000, 70000, 20).round(2),
    "HireDate": pd.date_range(start='2020-01-01', periods=20, freq='90D').strftime('%Y-%m-%d').tolist(),
    "EmploymentStatus": np.random.choice(['Full-time', 'Part-time'], 20, p=[0.5, 0.5]),
    "ExpectedWorkingHour": np.random.choice([40, 20], 20, p=[0.5, 0.5])
}

# Creating DataFrame
employees_df_ny = pd.DataFrame(employees_data_ny)

# Generate realistic work hours for 600 shifts
def generate_realistic_shift(start_date, num_shifts, num_employees):
    shifts = []
    for i in range(num_shifts):
        employee_id = (i % num_employees) + 1  # Cycle through employee IDs
        store_id = np.random.randint(1, 6)
        start_time = start_date + pd.to_timedelta(np.random.randint(8, 16), unit='h')  # staggered start times
        end_time = start_time + pd.to_timedelta(np.random.randint(8, 11), unit='h')  # 8-10 hour shifts
        shifts.append((employee_id, store_id, start_time, end_time))
    return shifts

# Generate 600 realistic shifts for 20 employees
realistic_shifts = generate_realistic_shift(pd.Timestamp('2023-01-01'), 600, 20)

# Creating sample data for Work Hours (Shifts) with realistic shifts
shifts_data = {
    "WorkHoursID": range(1, 601),
    "EmployeeID": [shift[0] for shift in realistic_shifts],
    "StoreID": [shift[1] for shift in realistic_shifts],
    "StartTime": [shift[2].strftime('%Y-%m-%d %H:%M:%S') for shift in realistic_shifts],
    "EndTime": [shift[3].strftime('%Y-%m-%d %H:%M:%S') for shift in realistic_shifts]
}

# Creating DataFrame for realistic shifts
shifts_df = pd.DataFrame(shifts_data)

# Merging DataFrames with the updated employees data
merged_df = pd.merge(
    employees_df_ny,
    shifts_df,
    on=["EmployeeID", "StoreID"],
    how="inner"
)

# Selecting and renaming columns for the final output
final_df = merged_df[[
    "EmployeeID", "FirstName", "LastName", "JobTitle", "StoreID", "Salary",
    "HireDate", "EmploymentStatus", "ExpectedWorkingHour", "StartTime", "EndTime"
]]

# Save to Excel
file_path_employees_shifts = "Employees_and_Shifts_NY.xlsx"
final_df.to_excel(file_path_employees_shifts, index=False)

# Define realistic ranges for monthly expenses
expense_ranges = {
    "Utility": (1500, 3000),
    "Marketing": (500, 1500),
    "Security": (400, 800),
    "Supplies": (700, 1200),
    "Maintenance": (900, 2000),
    "OtherExpense": (100, 500)
}

# Function to generate realistic monthly expenses
def generate_realistic_expenses(expense_range):
    return [round(np.random.uniform(expense_range[0], expense_range[1]), 2) for _ in range(12)]

# Create realistic monthly expenses for each store for a year
monthly_expenses_data = {
    "StoreID": [],
    "Month": [],
    "Utility": [],
    "Marketing": [],
    "Security": [],
    "Supplies": [],
    "Maintenance": [],
    "OtherExpense": []
}

months = pd.date_range(start='2023-01-01', periods=12, freq='MS').strftime("%Y-%m").tolist()

for store_id in stores_df_ny["StoreID"].unique():
    utilities = generate_realistic_expenses(expense_ranges["Utility"])
    marketing = generate_realistic_expenses(expense_ranges["Marketing"])
    security = generate_realistic_expenses(expense_ranges["Security"])
    supplies = generate_realistic_expenses(expense_ranges["Supplies"])
    maintenance = generate_realistic_expenses(expense_ranges["Maintenance"])
    other_expense = generate_realistic_expenses(expense_ranges["OtherExpense"])
    
    for month in months:
        monthly_expenses_data["StoreID"].append(store_id)
        monthly_expenses_data["Month"].append(month)
        monthly_expenses_data["Utility"].append(utilities.pop(0))
        monthly_expenses_data["Marketing"].append(marketing.pop(0))
        monthly_expenses_data["Security"].append(security.pop(0))
        monthly_expenses_data["Supplies"].append(supplies.pop(0))
        monthly_expenses_data["Maintenance"].append(maintenance.pop(0))
        monthly_expenses_data["OtherExpense"].append(other_expense.pop(0))

# Creating DataFrame for updated monthly expenses
monthly_expenses_df_updated = pd.DataFrame(monthly_expenses_data)

# Merge updated monthly expenses with store details
combined_df_updated = pd.merge(
    stores_df_ny.drop(columns=["Utility", "Marketing", "Security", "Supplies", "Maintenance", "OtherExpense"]),
    monthly_expenses_df_updated,
    on=["StoreID"],
    how="inner"
)

# Save the combined DataFrame to an Excel file
file_path_combined_updated = "Combined_Stores_and_Updated_Realistic_Monthly_Expenses.xlsx"
combined_df_updated.to_excel(file_path_combined_updated, index=False)