from __future__ import annotations

from sparkos.application.workbench import WorkbenchService

def run_terminal_chat(service: WorkbenchService) -> None:
    print("AGI-吉尔伽美什 plain mode")
    print(f"model: {service.model_name} | used token: {service.used_tokens}")
    print("Normal chat by default. Use @file to enter task mode. Type /quit to exit.")
    while True:
        try:
            user_request = input("\nsparkos > ").strip()
        except (EOFError, KeyboardInterrupt):
            print()
            return

        if not user_request:
            continue
        if user_request in {"/q", "/quit", "exit"}:
            return

        print()
        for chunk in service.stream_input(user_request):
            print(chunk, end="", flush=True)
        print()
        if service.last_model_error:
            print(f"\nmodel fallback: {service.last_model_error}")
        print(f"used token: {service.used_tokens}")
