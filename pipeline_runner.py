import subprocess
import sys

def run(script):
    print(f"Running {script}...")
    result = subprocess.run([sys.executable, script])
    if result.returncode != 0:
        print(f"{script} failed.")
    else:
        print(f"{script} completed.\n")

if __name__ == "__main__":
    run("scraper.py")
    run("news_processor.py")
    run("intraday_engine.py")