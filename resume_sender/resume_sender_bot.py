import os
import aiosqlite
import sendgrid
import base64
from sendgrid.helpers.mail import Mail, Email, To, Content, Attachment, FileContent, FileName, FileType, Disposition
from aiogram import Bot, Dispatcher, types, executor
from aiogram.dispatcher import FSMContext
from aiogram.dispatcher.filters.state import State, StatesGroup
from aiogram.contrib.fsm_storage.memory import MemoryStorage
from config import BOT_TOKEN_RESUME, SENDGRID_APIKEY, EMAIL


BOT_TOKEN = BOT_TOKEN_RESUME
SENDGRID_API_KEY = SENDGRID_APIKEY
SENDER_EMAIL = EMAIL

bot = Bot(token=BOT_TOKEN)
dp = Dispatcher(bot, storage=MemoryStorage())


class ResumeForm(StatesGroup):
    position = State()
    name = State()
    email = State()
    phone = State()
    resume = State()
    cv = State()


@dp.message_handler(commands=["start"])
async def start_command(message: types.Message):
    await message.answer("Welcome! Please enter the position you're applying for (e.g., Chief Officer):")
    await ResumeForm.position.set()


@dp.message_handler(state=ResumeForm.position)
async def get_position(message: types.Message, state: FSMContext):
    await state.update_data(position=message.text)
    await message.answer("Please enter your full name:")
    await ResumeForm.name.set()


@dp.message_handler(state=ResumeForm.name)
async def get_name(message: types.Message, state: FSMContext):
    await state.update_data(name=message.text)
    await message.answer("Please enter your email address:")
    await ResumeForm.email.set()


@dp.message_handler(state=ResumeForm.email)
async def get_email(message: types.Message, state: FSMContext):
    await state.update_data(email=message.text)
    await message.answer("Please enter your phone number:")
    await ResumeForm.phone.set()


@dp.message_handler(state=ResumeForm.phone)
async def get_phone(message: types.Message, state: FSMContext):
    await state.update_data(phone=message.text)
    await message.answer("Please upload your Resume (PDF):")
    await ResumeForm.resume.set()


@dp.message_handler(content_types=types.ContentType.DOCUMENT, state=ResumeForm.resume)
async def get_resume(message: types.Message, state: FSMContext):
    resume_file = await message.document.download()
    await state.update_data(resume_path=resume_file.name)
    await message.answer("Please upload your CV (PDF):")
    await ResumeForm.cv.set()


@dp.message_handler(content_types=types.ContentType.DOCUMENT, state=ResumeForm.cv)
async def get_cv(message: types.Message, state: FSMContext):
    cv_file = await message.document.download()
    await state.update_data(cv_path=cv_file.name)

    user_data = await state.get_data()
    await message.answer("Sending emails now... Please wait.")

    await send_bulk_emails(user_data, message)

    await message.answer("All emails sent successfully.")
    await state.finish()


async def send_bulk_emails(data, message):
    position = data["position"]
    resume_path = data["resume_path"]
    cv_path = data["cv_path"]
    user_name = data["name"]
    user_email = data["email"]
    user_phone = data["phone"]
    # Добавьте в начало кода
    print(f"Using sender: {SENDER_EMAIL}, API key: {SENDGRID_API_KEY[:5]}...")
    print("Preparing to send emails...")
    print(f"Position: {position}")
    print(f"Resume path: {resume_path}")
    print(f"CV path: {cv_path}")
    print(f"User name: {user_name}")
    print(f"User email: {user_email}")
    print(f"User phone: {user_phone}")

    html = f"""
    <html>
        <body>
            <p>Dear Crewing Manager,<br><br>
            I am writing to express my interest in the <strong>{position}</strong> position onboard your good vessel.<br>
            Please find attached my Resume and CV.<br><br>

            My details are as follows:<br>
            <strong>Name:</strong> {user_name}<br>
            <strong>Email:</strong> {user_email}<br>
            <strong>Phone:</strong> {user_phone}<br><br>

            Best regards,<br>
            {user_name}
            </p>
        </body>
    </html>
    """

    try:
        sg = sendgrid.SendGridAPIClient(api_key=SENDGRID_API_KEY)
        print("SendGrid client initialized.")

        async with aiosqlite.connect("crewing_2.db") as db:
            print("Connected to crewing_2.db database.")
            async with db.execute("SELECT email FROM companies") as cursor:
                print("Executing SQL query to fetch company emails...")
                async for row in cursor:
                    print("Fetched a new row from the database.")
                    to_email = row[0]
                    print(f"Sending to: {to_email}")

                    # Compose the email
                    from_email = Email(SENDER_EMAIL)
                    to_email = To(to_email)
                    subject = f"Application for {position} position"
                    content = Content("text/html", html)

                    def create_attachment(file_path: str, name: str) -> Attachment:
                        with open(file_path, "rb") as f:
                            data = f.read()
                        encoded_file = base64.b64encode(data).decode()
                        return Attachment(
                            FileContent(encoded_file),
                            FileName(name),
                            FileType("application/pdf"),
                            Disposition("attachment")
                        )

                    resume_attachment = create_attachment(resume_path, "Resume.pdf")
                    cv_attachment = create_attachment(cv_path, "CV.pdf")
                    print("Attachments created successfully.")

                    mail = Mail(from_email, to_email, subject, content)
                    mail.add_attachment(resume_attachment)
                    mail.add_attachment(cv_attachment)

                    # Send email
                    response = sg.send(mail)
                    print(f"SendGrid response status: {response.status_code}")

                    # Logging to confirm email sent
                    if response.status_code == 202:
                        await message.answer(f"Email sent to {to_email.email}")
                    else:
                        await message.answer(f"Failed to send email to {to_email}")

    except Exception as e:
        print(f"Exception occurred: {str(e)}")
        await message.answer(f"Error: {str(e)}")

async def main():
    await dp.start_polling()

if __name__ == "__main__":
    import asyncio
    asyncio.run(main())