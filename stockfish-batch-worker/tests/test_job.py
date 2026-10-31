import importlib.util
from pathlib import Path
import unittest

path = Path(__file__).resolve().parents[1] / "scripts/render_job.py"
spec = importlib.util.spec_from_file_location("render_job", path)
render_job = importlib.util.module_from_spec(spec)
spec.loader.exec_module(render_job)


class JobTests(unittest.TestCase):
    def setUp(self):
        self.image = render_job.REPOSITORY + "stockfish-analyzer@sha256:" + "a" * 64
        self.input = render_job.BUCKET + "inputs/smoke/input.jsonl"
        self.output = render_job.BUCKET + "results/smoke"

    def test_render_is_digest_pinned_and_bounded(self):
        job = render_job.render(self.image, self.input, self.output)
        group = job["taskGroups"][0]
        task = group["taskSpec"]
        self.assertEqual(group["taskCount"], "1")
        self.assertEqual(group["parallelism"], "1")
        self.assertEqual(task["maxRetryCount"], 0)
        self.assertEqual(task["maxRunDuration"], "900s")
        container = task["runnables"][0]["container"]
        self.assertEqual(container["imageUri"], self.image)
        options = dict(zip(container["commands"][::2], container["commands"][1::2]))
        self.assertEqual(options["--input"], self.input)
        self.assertEqual(options["--max-positions"], "20")
        self.assertEqual(int(options["--workers"]) * int(options["--threads"]), 4)
        self.assertLess(int(options["--run-timeout"]), 900)
        policy = job["allocationPolicy"]["instances"][0]["policy"]
        self.assertEqual(policy["provisioningModel"], "SPOT")
        self.assertEqual(policy["machineType"], "n2d-standard-4")
        self.assertIn("chessism-batch-worker@", job["allocationPolicy"]["serviceAccount"]["email"])

    def test_reject_unpinned_image_wrong_bucket_and_bad_paths(self):
        for image, source, destination in (
            (self.image.split("@")[0] + ":latest", self.input, self.output),
            (self.image, "gs://other/inputs/x", self.output),
            (self.image, self.input, render_job.BUCKET + "inputs/x"),
            (self.image, self.input, self.output + "/../x"),
            (self.image, self.input + "/", self.output),
            (self.image, self.input, render_job.BUCKET + "results/"),
        ):
            with self.subTest(source=source, destination=destination), self.assertRaises(ValueError):
                render_job.render(image, source, destination)


if __name__ == "__main__":
    unittest.main()
