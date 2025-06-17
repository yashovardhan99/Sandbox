import time
import shutil
import subprocess
import sys

cols = shutil.get_terminal_size().columns

powershell = shutil.which("powershell.exe")  # Check if PowerShell is available


def beep():
    if powershell:
        # Use PowerShell to play a beep sound
        subprocess.run([powershell, "-c", "[console]::beep(2000, 300)"], check=True)
    else:
        # Fallback to a simple BEL character
        print("\a", end="")


def timer(seconds):
    """Function to Count down from a specified number of seconds."""
    print(f"Starting timer for {seconds} seconds...")
    start = time.time()
    end = start + seconds
    while time.time() < end:
        remaining = end - time.time()
        mins, secs = divmod(remaining, 60)
        timer_format = f"Time Remaining: {int(mins):02d}:{int(secs):02d}"
        print(timer_format, end="\r")
        time.sleep(min(1, remaining))
    print(" " * cols, end="\r")  # Clear the line
    print("Time's up!")
    for _ in range(3):  # Beep 3 times
        beep()
        time.sleep(0.2)


if __name__ == "__main__":
    try:
        user_input = sys.argv[1] if len(sys.argv) > 1 else None

        if user_input is None:
            user_input = input("Enter the time in seconds or MM:SS format: ")

        if ":" in user_input:
            mins, secs = user_input.split(":")
            if not mins.isdigit() or not secs.isdigit():
                raise ValueError(
                    "Input in the format MM:SS must have both minutes and seconds as integers."
                )
            seconds = int(mins) * 60 + int(secs)
        elif user_input.isdigit():
            seconds = int(user_input)
        else:
            raise ValueError("Input must be a positive integer or in the format MM:SS.")

        timer(seconds)
    except ValueError as e:
        print(f"Invalid input: {e}")
    except KeyboardInterrupt:
        print("\nTimer cancelled.")
