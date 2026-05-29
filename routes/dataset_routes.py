from fastapi import (
    APIRouter,
    UploadFile,
    File,
    Form,
    HTTPException
)

from services.upload_service import (
    UploadService
)

router = APIRouter()

upload_service = UploadService()

# =====================================================
# UPLOAD DATASET
# =====================================================

@router.post("/datasets/upload")
async def upload_dataset(

    user_id: str = Form(...),

    file: UploadFile = File(...)
):

    try:

        result = await upload_service.upload_dataset(

            user_id=user_id,

            file=file
        )

        return result

    except Exception as e:

        print(
            f"❌ Dataset Route Error: "
            f"{str(e)}"
        )

        raise HTTPException(

            status_code=500,

            detail=str(e)
        )

