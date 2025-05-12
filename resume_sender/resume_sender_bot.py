import os
import base64
import aiosqlite
import tempfile
import sendgrid
from sendgrid.helpers.mail import (
    Mail, Email, To, Content, Attachment, FileContent, FileName, FileType, Disposition
)

from aiogram import Bot, Dispatcher, Router, types
from aiogram.fsm.state import State, StatesGroup
from aiogram.fsm.storage.memory import MemoryStorage
from aiogram.types import Message, FSInputFile
from aiogram.filters import CommandStart
from aiogram.fsm.context import FSMContext

from config import BOT_TOKEN_RESUME, SENDGRID_APIKEY, EMAIL

# Создание бота, диспетчера и роутера
bot = Bot(token=BOT_TOKEN_RESUME)
storage = MemoryStorage()
dp = Dispatcher(storage=storage)
router = Router()
dp.include_router(router)


class ResumeForm(StatesGroup):
    position = State()
    name = State()
    email = State()
    phone = State()
    resume = State()
    cv = State()


@router.message(CommandStart())
async def start_command(message: Message, state: FSMContext):
    print(f"Received start command from {message.from_user.id}")
    await message.answer("Добро пожаловать! Введите позицию, на которую подаёте резюме:")
    await state.set_state(ResumeForm.position)


@router.message(ResumeForm.position)
async def get_position(message: Message, state: FSMContext):
    print(f"Received position: {message.text}")
    await state.update_data(position=message.text)
    await message.answer("Введите ваше полное имя:")
    await state.set_state(ResumeForm.name)


@router.message(ResumeForm.name)
async def get_name(message: Message, state: FSMContext):
    print(f"Received name: {message.text}")
    await state.update_data(name=message.text)
    await message.answer("Введите ваш email:")
    await state.set_state(ResumeForm.email)


@router.message(ResumeForm.email)
async def get_email(message: Message, state: FSMContext):
    print(f"Received email: {message.text}")
    await state.update_data(email=message.text)
    await message.answer("Введите номер телефона:")
    await state.set_state(ResumeForm.phone)


@router.message(ResumeForm.phone)
async def get_phone(message: Message, state: FSMContext):
    print(f"Received phone number: {message.text}")
    await state.update_data(phone=message.text)
    await message.answer("Загрузите ваше резюме (PDF):")
    await state.set_state(ResumeForm.resume)


@router.message(ResumeForm.resume)
async def get_resume(message: Message, state: FSMContext):
    print("Received resume file")
    if not message.document or message.document.mime_type != "application/pdf":
        await message.answer("Пожалуйста, загрузите именно PDF-файл.")
        return

    with tempfile.NamedTemporaryFile(delete=False, suffix=".pdf") as temp:
        await message.bot.download(message.document.file_id, destination=temp)
        resume_path = temp.name
    print(f"Resume saved to {resume_path}")

    await state.update_data(resume_path=resume_path)
    await message.answer("Загрузите CV (PDF):")
    await state.set_state(ResumeForm.cv)


@router.message(ResumeForm.cv)
async def get_cv(message: Message, state: FSMContext):
    print("Received CV file")
    if not message.document or message.document.mime_type != "application/pdf":
        await message.answer("Пожалуйста, загрузите именно PDF-файл.")
        return

    if message.document.file_size > 5 * 1024 * 1024:
        await message.answer("Файл слишком большой. Максимум — 5 МБ.")
        return

    with tempfile.NamedTemporaryFile(delete=False, suffix=".pdf") as temp:
        await message.bot.download(message.document.file_id, destination=temp)
        cv_path = temp.name
    print(f"CV saved to {cv_path}")

    await state.update_data(cv_path=cv_path)
    user_data = await state.get_data()

    await message.answer("Отправка писем... Пожалуйста, подождите.")
    print("Sending emails...")

    await send_bulk_emails(user_data, message)

    os.remove(user_data["resume_path"])
    os.remove(cv_path)

    print(f"Removed files: {user_data['resume_path']} and {cv_path}")
    await message.answer("Резюме успешно разосланы всем компаниям.")
    await state.clear()


async def send_bulk_emails(data: dict, message: Message):
    print("Start sending bulk emails")
    sg = sendgrid.SendGridAPIClient(api_key=SENDGRID_APIKEY)

    html = f"""
    <p>Dear Crewing Manager,<br><br>
    I am applying for the <strong>{data['position']}</strong> position onboard your vessel.<br>
    Please find attached my Resume and CV.<br><br>
    <strong>Name:</strong> {data['name']}<br>
    <strong>Email:</strong> {data['email']}<br>
    <strong>Phone:</strong> {data['phone']}<br><br>
    Best regards,<br>
    {data['name']}
    </p>
    """

    def create_attachment(file_path: str, name: str) -> Attachment:
        with open(file_path, "rb") as f:
            encoded = base64.b64encode(f.read()).decode()
        return Attachment(
            FileContent(encoded),
            FileName(name),
            FileType("application/pdf"),
            Disposition("attachment")
        )

    async with aiosqlite.connect("crewing_2.db") as db:
        async with db.execute("SELECT email FROM companies") as cursor:
            async for row in cursor:
                to_email = row[0]
                try:
                    mail = Mail(
                        from_email=Email(EMAIL),
                        to_emails=To(to_email),
                        subject=f"Application for {data['position']} position",
                        html_content=Content("text/html", html)
                    )
                    mail.add_attachment(create_attachment(data["resume_path"], "Resume.pdf"))
                    mail.add_attachment(create_attachment(data["cv_path"], "CV.pdf"))
                    response = sg.send(mail)

                    if response.status_code == 202:
                        print(f"Email sent to: {to_email}")
                        await message.answer(f"Письмо отправлено: {to_email}")
                    else:
                        print(f"Error sending email to {to_email}: {response.status_code}")
                        await message.answer(f"Ошибка при отправке на {to_email}: {response.status_code}")
                except Exception as e:
                    print(f"Error for {to_email}: {str(e)}")
                    await message.answer(f"Ошибка для {to_email}: {str(e)}")


# Точка входа
if __name__ == "__main__":
    import asyncio

    async def main():
        print("Bot is starting...")
        await dp.start_polling(bot)

    asyncio.run(main())