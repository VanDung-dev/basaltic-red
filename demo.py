import os
import sys
import time
import basaltic_red as br

def main():
    target = (sys.argv[1] if len(sys.argv) > 1 else input("Enter file/directory path: ")).strip("'\" ")
    if not os.path.exists(target):
        sys.exit(f"Error: Path '{target}' not found.")

    engine = br.MatrixEngine()
    start_time = time.time()

    if os.path.isdir(target):
        files, total, clean, trash = engine.process_and_write_lake(target, "output/clean", "output/trash", None, 65536)
        elapsed = time.time() - start_time
        print(f"\n[Data Lake Directory: '{target}']")
        print(f"  Files   : {files}")
        print(f"  Total   : {total:,}")
        print(f"  Clean   : {clean:,} ({clean/total*100:.1f}%)" if total else "  Clean   : 0")
        print(f"  Trash   : {trash:,} ({trash/total*100:.1f}%)" if total else "  Trash   : 0")
        print(f"  Time    : {elapsed:.2f}s")
    else:
        size_mb = os.path.getsize(target) / (1024 ** 2)
        total, clean, trash = engine.process_file(target, 65536)
        elapsed = time.time() - start_time
        speed = size_mb / elapsed if elapsed > 0 else 0

        print(f"\n[Single File: '{target}' ({size_mb:.2f} MB)]")
        print(f"  Total   : {total:,}")
        print(f"  Clean   : {clean:,} ({clean/total*100:.1f}%)" if total else "  Clean   : 0")
        print(f"  Trash   : {trash:,} ({trash/total*100:.1f}%)" if total else "  Trash   : 0")
        print(f"  Speed   : {speed:.2f} MB/s ({elapsed:.2f}s)")

        # Preview & Data Dictionary
        try:
            c_b, t_b = engine.preview_sample(target, 100)
            print(f"  Preview : Clean {c_b.num_rows} rows | Trash {t_b.num_rows} rows")
        except Exception:
            pass

        try:
            md_path = engine.export_data_dictionary_md(target, "data_dictionary_demo.md")
            print(f"  Dict MD : Exported to '{md_path}'")
        except Exception:
            pass

if __name__ == "__main__":
    main()
