import pathlib
import runpy
import threading
import types
import unittest


HELPER = runpy.run_path(str(pathlib.Path(__file__).with_name("session-helper")))


class KernelTest(unittest.TestCase):
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


if __name__ == "__main__":
    unittest.main()
