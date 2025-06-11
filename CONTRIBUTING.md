# 🚀 Contributing to TurboLFS

Thank you for your interest in contributing to **TurboLFS**! Follow the steps below to get the project running locally.

---

## 🛠️ Setup Instructions

1. **Clone the Repository**
   Open a terminal and run:

   ```bash
   git clone https://github.com/TurboLFS/TurboLFS
   cd TurboLFS
   ```

2. **Install Dependencies**
   Use `yarn` to install all required packages:

   ```bash
   yarn
   ```

3. **Start the TurboLFS Server**
   In a **separate terminal window** (PowerShell, Terminal, or Command Prompt), run the following command:

   ```bash
   node src turbo server --port 3000 --objects-repo=TurboLFS/lfs-test --object-repo-token=PUBLIC
   ```

   This will launch a local TurboLFS server acting as a Git LFS proxy.

4. **Clone the LFS Test Repository**
   In your original terminal, run:

   ```bash
   node src clone https://github.com/TurboLFS/lfs-test
   ```

   > ⚠️ If the cloning process fails, try enabling Git trace logging for more detailed error output:

   * **In PowerShell**:

     ```bash
     $env:GIT_TRACE=1
     node src clone https://github.com/TurboLFS/lfs-test
     ```

---

## ✅ You're All Set!

If everything runs smoothly, you're now ready to explore, develop, and contribute to TurboLFS!

Feel free to open issues or pull requests if you spot bugs or want to suggest improvements. Happy coding! 💻
