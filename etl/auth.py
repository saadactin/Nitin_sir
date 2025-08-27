import functools
from flask import Flask, session, redirect, url_for, request, render_template, flash

# ---------------- In-Memory Users ----------------
# Predefined users for demonstration
users = {
    "admin": {"password": "admin123", "role": "admin"},
    "operator1": {"password": "operator123", "role": "operator"},
    "viewer1": {"password": "viewer123", "role": "viewer"}
}

# ---------------- Role-Based Access Decorator ----------------
def login_required(roles=None):
    """
    Decorator to enforce login and role-based access.
    roles: list of roles allowed to access this route
    """
    def decorator(func):
        @functools.wraps(func)
        def wrapper(*args, **kwargs):
            if "username" not in session:
                flash("Please login first", "warning")
                return redirect(url_for("login"))
            if roles and session.get("role") not in roles:
                flash("Access denied: insufficient permissions", "danger")
                return redirect(url_for("index"))
            return func(*args, **kwargs)
        return wrapper
    return decorator

# ---------------- Initialize Authentication ----------------
def init_auth(app: Flask):
    # Secret key for sessions
    app.secret_key = "supersecretkey"  # Change this in production

    # -------- Login Route --------
    @app.route("/login", methods=["GET", "POST"])
    def login():
        if request.method == "POST":
            username = request.form.get("username").strip()
            password = request.form.get("password").strip()
            user = users.get(username)
            if user and user["password"] == password:
                session["username"] = username
                session["role"] = user["role"]
                flash(f"Welcome, {username}!", "success")
                return redirect(url_for("index"))
            flash("Invalid username or password", "danger")
        return render_template("login.html")

    # -------- Logout Route --------
    @app.route("/logout")
    def logout():
        session.clear()
        flash("You have been logged out", "info")
        return redirect(url_for("login"))

    # -------- Admin-Only Create User Route --------
    @app.route("/create-user", methods=["GET", "POST"])
    @login_required(roles=["admin"])
    def create_user():
        if request.method == "POST":
            username = request.form.get("username").strip()
            password = request.form.get("password").strip()
            role = request.form.get("role").strip()

            # Validation
            if not username or not password or role not in ["admin", "operator", "viewer"]:
                flash("Invalid input", "danger")
            elif username in users:
                flash("User already exists", "danger")
            else:
                users[username] = {"password": password, "role": role}
                flash(f"User '{username}' ({role}) created successfully", "success")

        # Pass all users to the template to display
        return render_template("create_user.html", users=users)
