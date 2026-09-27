import os
import sys

import rpyc

SERVER_HOST = os.getenv("SERVER_HOST", "server")
SERVER_PORT = int(os.getenv("SERVER_PORT", "18861"))


def main():
    if len(sys.argv) == 3:
        keyword = sys.argv[1].strip()
        filename = sys.argv[2].strip()
    else:
        keyword = input("Keyword: ").strip()
        filename = input("File (.txt): ").strip()

    if not keyword:
        print("Please provide a keyword.")
        sys.exit(1)

    if not filename:
        print("Please provide a .txt filename.")
        sys.exit(1)

    try:
        conn = rpyc.connect(SERVER_HOST, SERVER_PORT)
        try:
            print(f"Server: {conn.root.ping()}")
            result = conn.root.count_word(keyword, filename)
            print(f"Keyword:   {result['keyword']}")
            print(f"File:      {result['filename']}")
            print(f"Count:     {result['count']}")
            print(f"Cache hit: {result['cache_hit']}")
        finally:
            conn.close()
    except Exception as exc:
        print(f"RPC request failed: {exc}")
        sys.exit(2)


if __name__ == "__main__":
    main()
