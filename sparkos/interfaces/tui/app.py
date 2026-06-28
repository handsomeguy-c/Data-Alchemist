from __future__ import annotations

import time

from textual import on
from textual.app import App, ComposeResult
from textual.containers import Horizontal, Vertical, VerticalScroll
from textual.widgets import Input, Static

from sparkos.application.workbench import WorkbenchService


class SparkOsApp(App):
    CSS = """
    Screen {
      background: #05070a;
      color: #d7dde7;
    }

    #shell {
      height: 100%;
      background: #05070a;
    }

    #topbar {
      height: 3;
      padding: 0 2;
      background: #080b10;
      border-bottom: solid #151b24;
      color: #9aa6b8;
    }

    #runtime {
      width: 1fr;
      content-align: left middle;
      text-style: bold;
      color: #f5f7fb;
    }

    #model {
      width: 1fr;
      content-align: center middle;
      color: #9aa6b8;
    }

    #tokens {
      width: auto;
      min-width: 22;
      content-align: right middle;
      color: #77d8c1;
    }

    #conversation {
      height: 1fr;
      margin: 1 2;
      padding: 1 2;
      background: #05070a;
      border: round #ff9500;
    }

    #composer {
      height: 4;
      padding: 0 2 1 2;
      background: #05070a;
    }

    #command {
      width: 1fr;
      height: 3;
      background: #090d13;
      color: #edf2f7;
      border: round #1b2430;
      padding: 0 1;
    }

    #command:focus {
      border: round #ff9500;
    }

    .message {
      margin: 0 0 1 0;
      padding: 0;
    }

    .message-user {
      color: #f5f7fb;
    }

    .message-agent {
      color: #ffd6a3;
    }

    .message-result {
      color: #ffd6a3;
    }

    .message-warn {
      color: #ff9500;
    }

    .meta {
      color: #657287;
      text-style: bold;
    }

    .body {
      color: #d7dde7;
    }
    """

    TITLE = "AGI-吉尔伽美什"
    SUB_TITLE = "Problem-first data and graph agent"
    BINDINGS = [
        ("ctrl+c", "quit", "Quit"),
        ("ctrl+l", "clear", "Clear"),
    ]

    def __init__(self, service: WorkbenchService):
        super().__init__()
        self._service = service

    def compose(self) -> ComposeResult:
        with Vertical(id="shell"):
            with Horizontal(id="topbar"):
                yield Static("", id="runtime")
                yield Static("", id="model")
                yield Static("", id="tokens")
            yield VerticalScroll(id="conversation")
            with Horizontal(id="composer"):
                yield Input(
                    placeholder="正常对话直接输入；任务处理请使用 @文件 并说明训练数据处理或向量知识库构建",
                    id="command",
                )

    def on_mount(self) -> None:
        self.query_one("#command", Input).focus()
        self._update_topbar("READY")
        self._append(
            "AGI-吉尔伽美什",
            "online",
            "默认是正常对话。只有输入 @文件 时，才进入数据工程任务处理模式。",
            "message-agent",
        )

    @on(Input.Submitted, "#command")
    def handle_input(self, event: Input.Submitted) -> None:
        value = event.value.strip()
        event.input.value = ""
        if not value:
            return

        if value in {"/q", "/quit", "exit"}:
            self.exit()
            return
        if value in {"/clear", "clear"}:
            self.action_clear()
            return
        self._append("user", "request", value, "message-user")
        response_widget = self._append(
            "AGI-吉尔伽美什",
            "stream",
            "",
            "message-agent",
        )
        self._update_topbar("THINKING")
        self.run_worker(
            lambda: self._stream_response(value, response_widget),
            thread=True,
            exclusive=False,
        )

    def action_clear(self) -> None:
        conversation = self.query_one("#conversation", VerticalScroll)
        conversation.remove_children()
        self._append(
            "system",
            "cleared",
            "上下文视图已清空，任务状态仍保留在左侧。",
            "message-warn",
        )

    def _append(self, actor: str, label: str, body: str, css_class: str) -> Static:
        conversation = self.query_one("#conversation", VerticalScroll)
        message = Static(
            f"[dim]{actor}[/] [bold]{label}[/]\n{body}",
            classes=f"message {css_class}",
        )
        conversation.mount(message)
        conversation.scroll_end(animate=False)
        return message

    def _stream_response(self, user_input: str, widget: Static) -> None:
        buffer = ""
        try:
            for chunk in self._service.stream_input(user_input):
                buffer += chunk
                self.call_from_thread(self._replace_message, widget, buffer)
                time.sleep(0.01)
        except Exception as exc:
            self.call_from_thread(self._update_topbar, "ERROR")
            self.call_from_thread(
                self._replace_message,
                widget,
                self._format_error(exc),
            )
            return

        if not buffer:
            self.call_from_thread(self._replace_message, widget, "没有生成响应。")
        self.call_from_thread(self._append_model_warning)
        self.call_from_thread(self._update_topbar, "READY")

    def _replace_message(self, widget: Static, body: str) -> None:
        widget.update(f"[dim]AGI-吉尔伽美什[/] [bold]stream[/]\n{body}")
        self.query_one("#conversation", VerticalScroll).scroll_end(animate=False)

    def _update_topbar(self, status: str) -> None:
        self.query_one("#runtime", Static).update(f"AGI-吉尔伽美什：{status}")
        self.query_one("#model", Static).update(f"model：{self._service.model_name}")
        self.query_one("#tokens", Static).update(
            f"used token：{self._service.used_tokens}"
        )

    def _format_error(self, exc: Exception) -> str:
        return (
            f"{type(exc).__name__}: {exc}\n"
            "请检查 config/config.yaml 的 master-model 三个槽位：model、url、api-key。"
        )

    def _append_model_warning(self) -> None:
        if not self._service.last_model_error:
            return
        self._append(
            "system",
            "model fallback",
            "主模型调用失败，已切换到本地规则模型。\n"
            f"{self._service.last_model_error}",
            "message-warn",
        )
