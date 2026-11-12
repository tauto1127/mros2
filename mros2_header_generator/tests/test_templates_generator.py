import subprocess
import sys
import tempfile
import unittest
from pathlib import Path


GENERATOR = Path(__file__).resolve().parents[1]


class TemplatesGeneratorTest(unittest.TestCase):
    def test_log_messages_do_not_mask_publisher_and_subscriber_calls(self):
        with tempfile.TemporaryDirectory() as temp:
            root = Path(temp)
            (root / 'mros2').symlink_to(GENERATOR.parent, target_is_directory=True)
            workspace = root / 'workspace'
            app = workspace / 'probe'
            app.mkdir(parents=True)
            (app / 'app.cpp').write_text(
                'auto pub = node.create_publisher<std_msgs::msg::String>("topic", 10);\n'
                'printf("event=create_publisher_begin topic=example");\n'
                'auto sub = node.create_subscription<std_msgs::msg::String>("topic", 10, callback);\n'
                'printf("event=create_subscription_done");\n',
                encoding='utf-8',
            )
            result = subprocess.run(
                [sys.executable, str(GENERATOR / 'templates_generator.py'), 'probe'],
                cwd=workspace,
                capture_output=True,
                text=True,
            )
            self.assertEqual(result.returncode, 0, result.stderr)
            generated = (app / 'templates.hpp').read_text(encoding='utf-8')
            self.assertIn('mros2::Node::create_publisher<std_msgs::msg::String>', generated)
            self.assertIn('mros2::Subscriber::callback_handler<std_msgs::msg::String>', generated)
            self.assertIn('#include "std_msgs/msg/string.hpp"', generated)

    def test_repeated_message_types_produce_one_instantiation_per_role(self):
        with tempfile.TemporaryDirectory() as temp:
            root = Path(temp)
            (root / 'mros2').symlink_to(GENERATOR.parent, target_is_directory=True)
            workspace = root / 'workspace'
            app = workspace / 'probe'
            app.mkdir(parents=True)
            (app / 'app.cpp').write_text(
                'auto a = node.create_publisher<std_msgs::msg::String>("a", 10);\n'
                'auto b = node.create_publisher<std_msgs::msg::String>("b", 10);\n'
                'auto c = node.create_subscription<std_msgs::msg::String>("c", 10, callback);\n'
                'auto d = node.create_subscription<std_msgs::msg::String>("d", 10, callback);\n',
                encoding='utf-8',
            )
            result = subprocess.run(
                [sys.executable, str(GENERATOR / 'templates_generator.py'), 'probe'],
                cwd=workspace,
                capture_output=True,
                text=True,
            )
            self.assertEqual(result.returncode, 0, result.stderr)
            generated = (app / 'templates.hpp').read_text(encoding='utf-8')
            self.assertEqual(generated.count('mros2::Node::create_publisher<std_msgs::msg::String>'), 1)
            self.assertEqual(generated.count('mros2::Subscriber::callback_handler<std_msgs::msg::String>'), 1)


if __name__ == '__main__':
    unittest.main()
