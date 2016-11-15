# Pyhton

These are only a few (out of many more) Python scripts I developed while working with tweets.
The neatest way to run them is following:
nohup python -u script.py > log_file.txt &
The -u option enables the output to be written into the log file right away.
Another thing I found useful is the Python code line by line:
sudo easy_install line_profiler
@profile
def main():...
Usage: kernprof -l -v script.py
