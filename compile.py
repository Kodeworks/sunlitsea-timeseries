import PyInstaller.__main__

PyInstaller.__main__.run([
    "dac.py",
    "-F",
    "-n", "download_orientation"
])
