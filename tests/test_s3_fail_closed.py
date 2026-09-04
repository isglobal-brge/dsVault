import unittest

from dsimaging_admin.s3 import delete_object_versions, is_loopback_s3_endpoint


class RecordingS3:
    def __init__(self):
        self.calls = []

    def delete_objects(self, **kwargs):
        self.calls.append(kwargs)
        return {}


class DeleteObjectVersionsContractTests(unittest.TestCase):
    def test_only_explicit_loopback_hosts_enable_local_controls(self):
        self.assertTrue(is_loopback_s3_endpoint("http://127.0.0.1:9000"))
        self.assertTrue(is_loopback_s3_endpoint("http://localhost:9000"))
        self.assertTrue(is_loopback_s3_endpoint("http://[::1]:9000"))
        self.assertFalse(is_loopback_s3_endpoint("https://minio.example.org"))
        self.assertFalse(is_loopback_s3_endpoint("https://localhost.example.org"))

    def test_rejects_any_entry_without_key_or_version_before_deleting(self):
        invalid_entries = (
            {"version_id": "version-1"},
            {"key": "datasets/study/image.nii.gz"},
            {"key": "datasets/study/image.nii.gz", "version_id": ""},
        )
        for invalid in invalid_entries:
            with self.subTest(invalid=invalid):
                s3 = RecordingS3()
                versions = [
                    {
                        "key": f"datasets/study/image-{index}.nii.gz",
                        "version_id": f"version-{index}",
                    }
                    for index in range(1000)
                ]
                versions.append(invalid)

                with self.assertRaisesRegex(ValueError, "Key and VersionId"):
                    delete_object_versions(s3, "imaging-data", versions)

                self.assertEqual(s3.calls, [])

    def test_valid_entries_are_still_deleted_in_batches(self):
        s3 = RecordingS3()
        versions = [{
            "key": f"datasets/study/image-{index}.nii.gz",
            "version_id": f"version-{index}",
        } for index in range(1001)]

        deleted = delete_object_versions(s3, "imaging-data", versions)

        self.assertEqual(deleted, 1001)
        self.assertEqual([len(call["Delete"]["Objects"]) for call in s3.calls], [1000, 1])


if __name__ == "__main__":
    unittest.main()
