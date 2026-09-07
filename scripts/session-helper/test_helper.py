import io
import pathlib
import queue
import runpy
import threading
import types
import unittest
from unittest.mock import Mock


HELPER = runpy.run_path(str(pathlib.Path(__file__).with_name("session-helper")))


class KernelTest(unittest.TestCase):
    def test_request_lines(self):
        kernel = types.SimpleNamespace(execute=Mock())
        request = b'{"op":"execute","cell_id":"cell","code":"1"}'
        stream = types.SimpleNamespace(makefile=lambda _: io.BytesIO(b"\n" + request + b"\n" + request))
        HELPER["serve_one"](kernel, None, "/work", stream)
        kernel.execute.assert_called_once_with("cell", "1")

    def test_output_order(self):
        for status, ename, expected in [("ok", "", "done"), ("error", "KeyboardInterrupt", "interrupted")]:
            with self.subTest(status=status):
                events = []
                settled = threading.Event()

                class Completion(threading.Condition):
                    def wait(self, timeout=None):
                        settled.set()
                        return super().wait(timeout)

                def send(event):
                    events.append(event)
                    if event.get("kind") == "cell":
                        settled.set()

                kernel = HELPER["Kernel"].__new__(HELPER["Kernel"])
                kernel.connection = types.SimpleNamespace(send=send)
                kernel.pending = {"request": "cell"}
                kernel.interrupted = set()
                kernel.interrupt_pending = set()
                kernel.lock = threading.Lock()
                kernel.completed = Completion(kernel.lock)
                kernel.idle = set()
                kernel.manager = types.SimpleNamespace(is_alive=lambda: True)
                kernel.client = types.SimpleNamespace(get_shell_msg=lambda **_: {
                    "parent_header": {"msg_id": "request"},
                    "content": {"status": status, "ename": ename, "execution_count": 1},
                })
                worker = threading.Thread(target=kernel._await_reply, args=("request", "cell"), daemon=True)
                worker.start()
                self.assertTrue(settled.wait(60))
                kernel._forward({
                    "msg_type": "stream", "parent_header": {"msg_id": "request"},
                    "content": {"name": "stdout", "text": "result"},
                })
                kernel._forward({
                    "msg_type": "status", "parent_header": {"msg_id": "request"},
                    "content": {"execution_state": "idle"},
                })
                worker.join(60)
                self.assertFalse(worker.is_alive())
                self.assertEqual([event["kind"] for event in events], ["output", "kernel", "cell"])
                self.assertEqual(events[-1]["state"], expected)
                self.assertEqual(kernel.pending, {})
                self.assertEqual(kernel.idle, set())

    def test_early_interrupt(self):
        kernel = HELPER["Kernel"].__new__(HELPER["Kernel"])
        kernel.connection = types.SimpleNamespace(send=Mock())
        kernel.pending = {"request": "cell"}
        kernel.generation = 0
        kernel.interrupted = set()
        kernel.interrupt_pending = set()
        kernel.cells = queue.Queue()
        kernel.lock = threading.Lock()
        kernel.completed = threading.Condition(kernel.lock)
        kernel.state = "idle"
        kernel.idle = set()
        kernel.manager = types.SimpleNamespace(interrupt_kernel=Mock(), is_alive=lambda: True)
        kernel.client = types.SimpleNamespace(get_shell_msg=lambda **_: {
            "parent_header": {"msg_id": "request"},
            "content": {"status": "error", "ename": "Error", "execution_count": 1},
        })
        kernel.interrupt()
        kernel.manager.interrupt_kernel.assert_not_called()
        for state in ["busy", "busy", "idle"]:
            kernel._forward({
                "msg_type": "status", "parent_header": {"msg_id": "request"},
                "content": {"execution_state": state},
            })
        kernel.manager.interrupt_kernel.assert_called_once()
        kernel._await_reply("request", "cell")
        self.assertEqual(kernel.connection.send.call_args.args[0]["state"], "interrupted")
        self.assertFalse(kernel.interrupted)
        self.assertFalse(kernel.interrupt_pending)

    def test_dequeued_interrupt(self):
        kernel = HELPER["Kernel"].__new__(HELPER["Kernel"])
        kernel.pending = {}
        kernel.generation = 0
        kernel.interrupted = set()
        kernel.interrupt_pending = set()
        kernel.lock = threading.Lock()
        kernel.state = "idle"
        kernel.client = types.SimpleNamespace(execute=Mock())
        kernel.manager = types.SimpleNamespace(interrupt_kernel=Mock())

        class Cells(queue.Queue):
            def get(self, block=True):
                if not block:
                    return super().get(block=False)
                if self.empty():
                    raise StopIteration
                cell = super().get()
                kernel.interrupt()
                return cell

        kernel.cells = Cells()
        kernel.execute("cell", "must not execute")
        with self.assertRaises(StopIteration):
            kernel._run_cells()
        kernel.client.execute.assert_not_called()
        kernel.manager.interrupt_kernel.assert_not_called()


if __name__ == "__main__":
    unittest.main()
