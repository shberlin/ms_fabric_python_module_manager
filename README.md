# ms_fabric_python_module_manager

This is a work-around solution to import python notebooks like python modules.
For more explanations check out my youtube video: https://www.youtube.com/watch?v=SqblTj7_zj8

# Quickstart

Steps:
1. Copy the code from "modules_helper" into a fabric notebook with the same name.
2. Create a lib folder in Fabric where you put all your utility notebooks. E.g. "utility", "custom_functions"

Now you are ready to go.

In you main/execution notebook, you need to add a first cell:

    %run modules_helper

In a second cell you can import your utility notebook like so:

    refresh_modules()
    import utility
    import custom_functions
    # ...

# Advanced

If you have multiple library folders (e.g. "utils", "helpers" and "modules") in fabric, you can run the refresh function like so:

    refresh_modules(relevant_folders=["utils", "helpers", "modules"])

