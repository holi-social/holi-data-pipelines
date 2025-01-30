from typing import cast
from faker import Faker
from src.pipelines.profiles_to_typesense.ingest_from_scratch import (
    UserRow,
    user_to_doc,
    users_to_typesense_docs,
    UserDoc,
)

fake = Faker()


def _gen_user_row() -> UserRow:
    user_id = fake.uuid4()
    return {
        "user_id": str(user_id),
        "first_name": fake.first_name(),
        "last_name": fake.last_name(),
        "location": fake.city(),
        "avatar": fake.image_url(),
    }


def test_user_to_doc() -> None:
    user_row = _gen_user_row()
    expected_doc = {
        "id": f"profile_{user_row['user_id']}",
        "type": "profile",
        "location": user_row["location"],
        "image_url": user_row["avatar"],
        "link_locators": {"profile": user_row["user_id"]},
        "user_full_name": f"{user_row['first_name']} {user_row['last_name']}",
    }
    assert user_to_doc(user_row) == expected_doc


def test_user_to_doc_only_uses_first_name_if_only_given() -> None:
    user_row = _gen_user_row()
    user_row["first_name"] = fake.first_name()
    user_row["last_name"] = None
    assert user_to_doc(user_row) is not None
    assert cast(UserDoc, user_to_doc(user_row)).get("user_full_name") == user_row["first_name"]


def test_user_to_doc_only_uses_last_name_if_only_given() -> None:
    user_row = _gen_user_row()
    user_row["first_name"] = None
    user_row["last_name"] = fake.last_name()
    assert user_to_doc(user_row) is not None
    assert cast(UserDoc, user_to_doc(user_row)).get("user_full_name") == user_row["last_name"]


def test_user_to_doc_strips_whitespace() -> None:
    user_row = _gen_user_row()
    first_name = fake.first_name()
    last_name = fake.last_name()
    user_row["first_name"] = " " + first_name
    user_row["last_name"] = " " + last_name + " "
    assert user_to_doc(user_row) is not None
    assert cast(UserDoc, user_to_doc(user_row)).get("user_full_name") == first_name + " " + last_name

    user_row["first_name"] = " "
    user_row["last_name"] = fake.last_name()
    assert user_to_doc(user_row) is not None
    assert cast(UserDoc, user_to_doc(user_row)).get("user_full_name") == user_row["last_name"]

    user_row["first_name"] = fake.first_name()
    user_row["last_name"] = " "
    assert user_to_doc(user_row) is not None
    assert cast(UserDoc, user_to_doc(user_row)).get("user_full_name") == user_row["first_name"]


def test_user_to_doc_returns_none_for_missing_name() -> None:
    user_row = _gen_user_row()
    user_row["first_name"] = None
    user_row["last_name"] = None
    assert user_to_doc(user_row) is None

    user_row["first_name"] = ""
    user_row["last_name"] = ""
    assert user_to_doc(user_row) is None

    user_row["first_name"] = " "
    user_row["last_name"] = " "
    assert user_to_doc(user_row) is None


def test_users_to_typesense_docs_filters_out_users_without_name() -> None:
    user_row_1 = _gen_user_row()
    user_row_2 = _gen_user_row()
    user_row_3 = _gen_user_row()

    user_row_2["first_name"] = None
    user_row_2["last_name"] = None

    result_list = users_to_typesense_docs([user_row_1, user_row_2, user_row_3])

    assert len(result_list) == 2
    assert result_list[0] == user_to_doc(user_row_1)
    assert result_list[1] == user_to_doc(user_row_3)
