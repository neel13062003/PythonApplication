import subprocess
import time
import os
import signal

# Path to the script you want to run
script_path = 'Email-Verification.py'


def terminate_existing_process():
    try:
        result = subprocess.check_output(
            ['tasklist', '/FI', 'IMAGENAME eq python.exe', '/FO', 'CSV'])
        processes = result.decode().split('\n')
        for process in processes:
            if script_path in process:
                parts = process.split(',')
                if len(parts) > 1:
                    pid = int(parts[1].strip('"'))
                    try:
                        os.kill(pid, signal.SIGTERM)
                        print(f'Terminated existing process with PID {pid}')
                    except Exception as e:
                        print(f'Error terminating process with PID {pid}: {e}')
    except subprocess.CalledProcessError as e:
        print(f'Error while fetching process list: {e}')


def run_script():
    process = subprocess.Popen(['python', script_path])         
    return process


def main():
    previous_process = None
    while True:
        # Terminate any existing process running the script
        if previous_process:
            previous_process.terminate()
            previous_process.wait()  # Ensure the process has terminated
            print('Previous process terminated')
            time.sleep(100)

        # Run the script
        process = run_script()
        previous_process = process

        # Wait for 2 seconds before running the script again
        time.sleep(3000)


if __name__ == '__main__':
    main()
