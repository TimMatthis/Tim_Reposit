import csv
import math
import datetime
import random
import os

# Configuration
CSV_FILE = "Copy of Monthly Report on filtered fleet_TM.csv"
OUTPUT_REPORT = "simulation_comparison.md"
DAYS_IN_YEAR = 365
INTERVALS_PER_DAY = 48 # 30-minute intervals
INTERVAL_HOURS = 0.5

# Synthetic Data Parameters
# Price: Low overnight, High evening peak, Low midday (solar sponge)
PRICE_PROFILE = [
    0.15, 0.15, 0.15, 0.15, 0.15, 0.15, 0.15, 0.15, # 00:00 - 04:00
    0.20, 0.20, 0.25, 0.25, 0.25, 0.25, # 04:00 - 07:00
    0.30, 0.30, 0.30, 0.30, # 07:00 - 09:00
    0.10, 0.10, 0.05, 0.05, 0.00, 0.00, 0.00, 0.00, 0.05, 0.05, 0.10, 0.10, # 09:00 - 15:00 (Solar Sponge)
    0.25, 0.25, 0.35, 0.35, 0.45, 0.45, 0.55, 0.55, # 15:00 - 19:00 (Peak)
    0.50, 0.50, 0.40, 0.40, 0.30, 0.30, 0.20, 0.20, 0.15, 0.15  # 19:00 - 00:00
]
# Feed-in Tariff (FiT)
FIT_RATE = 0.05 

class Battery:
    def __init__(self, capacity_kwh, max_power_kw, efficiency=0.9):
        self.capacity = float(capacity_kwh)
        self.max_power = float(max_power_kw)
        self.efficiency = efficiency
        self.soc = 0.0 # Start empty

    def update(self, power_kw, duration_hours):
        # Positive power = charging, Negative = discharging
        energy_kwh = power_kw * duration_hours
        
        if power_kw > 0:
            # Charging losses
            added_energy = energy_kwh * self.efficiency
            self.soc = min(self.soc + added_energy, self.capacity)
        else:
            # Discharging losses (energy taken from battery is more than delivered)
            removed_energy = abs(energy_kwh) / self.efficiency
            self.soc = max(self.soc - removed_energy, 0.0)

def generate_daily_profiles(day_of_year, total_daily_consumption, total_daily_generation):
    # Solar: Sine wave between 6am and 6pm
    solar_profile = []
    for i in range(INTERVALS_PER_DAY):
        hour = i / 2
        if 6 <= hour <= 18:
            # Simple sine wave
            val = math.sin((hour - 6) * math.pi / 12)
        else:
            val = 0
        solar_profile.append(val)
    
    # Normalize solar
    solar_sum = sum(solar_profile)
    if solar_sum > 0:
        solar_profile = [s * total_daily_generation / solar_sum for s in solar_profile]
    
    # Load: Double peak (Morning 7-9am, Evening 6-9pm) + baseload
    load_profile = []
    for i in range(INTERVALS_PER_DAY):
        hour = i / 2
        val = 0.1 # Baseload
        # Morning peak
        if 7 <= hour <= 9:
            val += 0.4
        # Evening peak
        if 18 <= hour <= 21:
            val += 0.6
        # Random noise
        val += random.uniform(0, 0.1)
        load_profile.append(val)
        
    # Normalize load
    load_sum = sum(load_profile)
    if load_sum > 0:
        load_profile = [l * total_daily_consumption / load_sum for l in load_profile]
        
    return solar_profile, load_profile

def optimize_day(battery, solar_profile, load_profile, price_profile):
    # Greedy Strategy:
    # 1. Calculate Net Load (Load - Solar)
    # 2. Identify "Excess Solar" intervals (Net Load < 0) -> Charge Battery
    # 3. Identify "Peak Price" intervals -> Discharge Battery to reduce Load or Export
    
    # Simplified logic for "Digital Twin" best strategy:
    # - Charge from excess solar first (free energy).
    # - If battery not full, charge from grid during cheapest intervals.
    # - Discharge during most expensive intervals.
    
    schedule = [0.0] * INTERVALS_PER_DAY
    net_load = [l - s for l, s in zip(load_profile, solar_profile)]
    
    # Pass 1: Charge from Excess Solar
    for i in range(INTERVALS_PER_DAY):
        if net_load[i] < 0:
            # Excess solar available
            excess_power = abs(net_load[i]) / INTERVAL_HOURS
            charge_power = min(excess_power, battery.max_power)
            
            # Check capacity limit
            max_energy_in = (battery.capacity - battery.soc) / battery.efficiency
            max_power_in = max_energy_in / INTERVAL_HOURS
            
            actual_charge = min(charge_power, max_power_in)
            
            if actual_charge > 0:
                battery.update(actual_charge, INTERVAL_HOURS)
                schedule[i] = actual_charge # Positive = Charging
                net_load[i] += actual_charge # Consuming excess solar
    
    # Pass 2: Discharge during Peak Prices
    # Sort intervals by price (descending)
    sorted_indices = sorted(range(INTERVALS_PER_DAY), key=lambda k: price_profile[k], reverse=True)
    
    for i in sorted_indices:
        if battery.soc > 0 and price_profile[i] > FIT_RATE: # Only discharge if price is good
            # Determine max discharge
            max_energy_out = battery.soc * battery.efficiency
            max_power_out = max_energy_out / INTERVAL_HOURS
            
            discharge_power = min(battery.max_power, max_power_out)
            
            if discharge_power > 0:
                battery.update(-discharge_power, INTERVAL_HOURS)
                schedule[i] = -discharge_power # Negative = Discharging
                net_load[i] -= discharge_power # Reducing load / Exporting
                
    return net_load

def run_simulation():
    results = []
    
    print(f"Reading {CSV_FILE}...")
    try:
        with open(CSV_FILE, 'r') as f:
            reader = csv.DictReader(f)
            rows = list(reader)
    except FileNotFoundError:
        print(f"Error: {CSV_FILE} not found.")
        return

    print(f"Simulating {len(rows)} sites...")
    
    for row in rows:
        uid = row['uid']
        try:
            # Parse site params
            # Handle potentially missing or malformed data
            sys_power = float(row.get('SystemPower', 5.0) or 5.0)
            batt_cap = float(row.get('UsableCapacity', 10.0) or 10.0)
            actual_bill = float(row.get('TotalBillCost', 0.0) or 0.0)
            
            # Estimate annual totals from monthly columns if available, or just use the TotalBillCost context
            # For simplicity in this "Digital Twin", we'll infer annual consumption/generation
            # from the monthly columns present in the CSV.
            # The CSV has columns like 'Dec24_Consumption_kWh', 'Jan25_Consumption_kWh', etc.
            
            total_consumption = 0.0
            total_generation = 0.0 # We'll infer this
            
            months = ['Dec24', 'Jan25', 'Feb25', 'Mar25', 'Apr25', 'May25', 
                      'Jun25', 'Jul25', 'Aug25', 'Sep25', 'Oct25', 'Nov25']
            
            for m in months:
                cons = float(row.get(f'{m}_Consumption_kWh', 0.0) or 0.0)
                imp_r = float(row.get(f'{m}_Imports_R_kWh', 0.0) or 0.0)
                imp_l = float(row.get(f'{m}_Imports_L_kWh', 0.0) or 0.0)
                exp_r = float(row.get(f'{m}_Exports_R_kWh', 0.0) or 0.0)
                exp_l = float(row.get(f'{m}_Exports_L_kWh', 0.0) or 0.0)
                
                total_consumption += cons
                # Generation approx = Consumption + Exports - Imports
                # (This is a rough approximation for the simulation scale)
                gen = cons + (abs(exp_r) + abs(exp_l)) - (imp_r + imp_l)
                total_generation += max(0, gen)

            # Daily averages
            avg_daily_cons = total_consumption / DAYS_IN_YEAR
            avg_daily_gen = total_generation / DAYS_IN_YEAR
            
            # Initialize Battery
            # Max power often ~0.5C or 5kW for residential
            batt = Battery(batt_cap, 5.0) 
            
            simulated_bill = 0.0
            
            # Run 365 days
            for day in range(DAYS_IN_YEAR):
                # Generate profiles with some randomness
                daily_cons = avg_daily_cons * random.uniform(0.8, 1.2)
                daily_gen = avg_daily_gen * random.uniform(0.5, 1.5) # Weather variance
                
                solar, load = generate_daily_profiles(day, daily_cons, daily_gen)
                
                # Optimize
                final_net_load = optimize_day(batt, solar, load, PRICE_PROFILE)
                
                # Calculate Cost
                daily_cost = 0.0
                for i in range(INTERVALS_PER_DAY):
                    power = final_net_load[i]
                    energy = power * INTERVAL_HOURS
                    price = PRICE_PROFILE[i]
                    
                    if energy > 0: # Import
                        daily_cost += energy * price
                    else: # Export
                        daily_cost += energy * FIT_RATE # Negative cost (credit)
                
                simulated_bill += daily_cost
            
            # Store result
            results.append({
                'uid': uid,
                'address': row.get('Address', 'Unknown'),
                'actual_bill': actual_bill,
                'simulated_bill': simulated_bill,
                'savings': actual_bill - simulated_bill
            })
            
        except Exception as e:
            import traceback
            traceback.print_exc()
            print(f"Skipping site {uid}: {e}")
            continue

    # Generate Report
    with open(OUTPUT_REPORT, 'w') as f:
        f.write("# Simulation Comparison Report\n\n")
        f.write("| UID | Address | Actual Bill ($) | Simulated Optimal Bill ($) | Potential Savings ($) |\n")
        f.write("|---|---|---|---|---|\n")
        
        total_actual = 0
        total_sim = 0
        
        for r in results:
            f.write(f"| {r['uid'][:8]}... | {r['address'][:30]}... | {r['actual_bill']:.2f} | {r['simulated_bill']:.2f} | {r['savings']:.2f} |\n")
            total_actual += r['actual_bill']
            total_sim += r['simulated_bill']
            
        f.write(f"\n**Total Actual Bill**: ${total_actual:.2f}\n")
        f.write(f"**Total Simulated Bill**: ${total_sim:.2f}\n")
        f.write(f"**Total Potential Savings**: ${total_actual - total_sim:.2f}\n")

    # Export to JSON for Dashboard
    import json
    json_output = "simulation_results.json"
    with open(json_output, 'w') as f:
        json.dump(results, f, indent=2)
    print(f"Results exported to {json_output}")

    print(f"Simulation complete. Report saved to {OUTPUT_REPORT}")

if __name__ == "__main__":
    run_simulation()
