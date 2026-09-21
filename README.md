# COM-490 Final Project: Robust Public Transport Routing Engine
**Group X1** | École Polytechnique Fédérale de Lausanne (EPFL)
*Team: Matthias Wyss, Thierry Sokhn, Léan Bruttin, Alain Girard, and Sofia Taouhid*

## Executive Summary
This repository contains our solution for the COM-490 final assignment: a **robust SBB journey planner**. Traditional public transport applications optimize solely for the shortest travel time, often suggesting risky connections. Our engine solves this by guaranteeing an arrival time with a user-defined statistical confidence level (Q%).

## 1. Problem Motivation & Assignment Context
Given a desired **day of the week**, **arrival time**, and **confidence level** (Q%), the route planner computes the fastest route (latest departure) between a given departure and arrival stop such that the probability of arriving on time is at least Q%. 
* We utilize SBB historical actual data (*IstDaten*) and timetable data (*GTFS*).
* The graph is constrained to the Lausanne and Ouest Lausannois districts.
* Walking transfers are permitted up to a maximum distance of 500m with an assumed speed of 50 m/min.

## 2. Our Solution & Architecture
Our solution consists of a modular architecture designed to handle massive data via PySpark and Trino, coupled with a robust optimization algorithm.

### The Routing Algorithm (`JourneyPlanner.py`)
We implemented a **Backward Stochastic Multi-Criteria Time-Dependent Dijkstra** algorithm. 
* **Backward Search:** By starting at the destination at the target arrival time and traversing backwards through time, the algorithm naturally discovers the *latest possible departure time*.
* **Pareto Dominance Frontier:** We optimize for three conflicting criteria simultaneously: Departure Time (maximize), Path Confidence (maximize), and Total Walking Distance (minimize). Inferior branches are dynamically pruned to ensure only optimal, non-redundant itineraries are returned.

### The Predictive Model (`DelayModel.py`)
To estimate connection reliability and elegantly handle data sparsity, we designed an empirical **5-Level Hierarchical Fallback Delay Model**:
1. **Level 1:** Trip + Stop + Weather (Highest contextual precision)
2. **Level 2:** Trip + Stop (Weather-agnostic)
3. **Level 3:** Stop + Weather (Station-level contextual average)
4. **Level 4:** Stop (Station-level generic average)
5. **Level 5:** Global Network (Absolute statistical safety net)

### Validation (`validate.py` & `validate_rain.py`)
We backtested our planner against historical SBB *IstDaten*. The results confirm that our engine successfully meets the specified confidence levels in real-world scenarios. We also conducted a statistical analysis of rainfall versus delays, which showed no significant systemic correlation in the dataset, though our model dynamically adapts to adverse weather contexts when granular data is present.

### Interactive Visualization (`visualization.py`)
The project includes an interactive UI built with Plotly and `ipywidgets`. It displays the Pareto-optimal routes on an interactive Mapbox interface, detailing transit paths, walking transfers, and connection waiting times.

## 3. How to Run
The core execution flow, model building, and demonstration are contained within a single Jupyter Notebook. **This project must be run within the EPFL COM-490 Jupyter Hub environment**, as it relies on specific environment variables, Hadoop (HDFS) clusters, and Trino database connections.

**Steps:**
1. Clone this repository into your COM-490 Jupyter Hub workspace.
2. Open **`project.ipynb`**.
3. Run the notebook cells sequentially.
   * *Note: The first execution builds the topological graph and delay models in Trino/Spark, which takes a moment to process. Subsequent runs will automatically load cached parquet files to drastically speed up execution.*
4. Interact with the Dashboard generated at the end of the notebook to test different routing queries, time constraints, and weather conditions.

## 4. Repository Structure
* `project.ipynb`: Main orchestrator notebook and demonstration dashboard.
* `JourneyPlanner.py`: Graph preparation and core backward-search routing algorithm.
* `DelayModel.py`: PySpark data alignment and hierarchical delay distribution model.
* `visualization.py`: Plotly/Mapbox interactive UI components.
* `validate.py` / `validate_rain.py`: Backtesting and weather correlation scripts.
* `/figs/`: Directory containing output plots (e.g., rainfall correlation histogram).