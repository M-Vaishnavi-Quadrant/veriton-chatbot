def generate_unique_job_name(
    base_name,
    existing_names
):

    existing_names = {

        str(name).strip().lower()

        for name in existing_names

        if name
    }

    if base_name.strip().lower() not in existing_names:

        return base_name

    counter = 2

    while True:

        candidate = (
            f"{base_name}_{counter}"
        )

        if candidate.lower() not in existing_names:

            return candidate

        counter += 1
