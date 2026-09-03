from dsimaging_admin.hashing import is_image_file, sample_id_from_filename


def test_jpeg_extension_is_recognized_case_insensitively():
    assert is_image_file("case001.jpeg")
    assert is_image_file("case002.JPEG")
    assert sample_id_from_filename("case001.jpeg") == "case001"
    assert sample_id_from_filename("case002.JPEG") == "case002"
