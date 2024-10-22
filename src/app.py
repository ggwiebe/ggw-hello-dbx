import os
from flask import Flask, make_response, render_template, request, jsonify
from werkzeug.utils import secure_filename, send_file
from databricks.sdk import WorkspaceClient
from werkzeug.middleware.proxy_fix import ProxyFix

app = Flask(__name__)

app.wsgi_app = ProxyFix(
    app.wsgi_app, x_for=1, x_proto=1, x_host=1, x_prefix=1
)

app.config["UPLOAD_FOLDER"]       = os.getenv("VOLUME_MOUNT_PATH", "/tmp")
app.config["VOLUME_URI"]          = os.getenv("VOLUME_URI", "/Volumes/ggw/apps/upload-app")
app.config["MAX_CONTENT_LENGTH"]  = 16 * 1024 * 1024
app.config["WORKSPACE_PROFILE"]   = os.getenv("WORKSPACE_PROFILE", "DEFAULT")
app.config["DATABRICKS_APP_PORT"] = os.getenv("DATABRICKS_APP_PORT", "8666")

print("working on folder " + app.config["UPLOAD_FOLDER"])
print("using workspace profile: " + app.config["WORKSPACE_PROFILE"])

# ws_profile = app.config["WORKSPACE_PROFILE"] # AWS_E2_FIELD_ENG_WEST
# if (ws_profile == null):
#     print(f"WORKSPACE_PROFILE app.config value not found; using DEFAULT")
#     ws_profile = "DEFAULT"
# print(f"creating workspace client using profile: {ws_profile}...")
w = WorkspaceClient(profile=app.config["WORKSPACE_PROFILE"])


@app.errorhandler(413)
def too_large(e):
    return make_response(jsonify(message="File is too large"), 413)


@app.route("/")
def home():
    print("called home")
    # TODO
    try:
        w.files.get_directory_metadata(app.config["VOLUME_URI"])
    except Exception as e:
        print(f'Directory error: {str(e)}')

    files = w.files.list_directory_contents(app.config["VOLUME_URI"])

    return render_template("upload.html", filenames=[f.name for f in files])


@app.route("/upload", methods=["GET", "POST"])
def uploader():
    if "file" not in request.files:
        return "No file part in the request", 400
    if request.method == "POST":
        f = request.files["file"]
        if f.filename == "":
            return "No selected file", 400
        try:
            filename = secure_filename(f.filename)
            w.files.upload(file_path=f'{app.config["VOLUME_URI"]}/{filename}', contents=f.stream, overwrite=True)
        except Exception as e:
            return f'Error saving file: {str(e)}', 500
        return "file uploaded successfully"


@app.route("/downloads/<filename>")
def download_file(filename):
    safe_filename = secure_filename(filename)
    response = w.files.download(f'{app.config["VOLUME_URI"]}/{safe_filename}')

    # save the file to the uploads folder
    return send_file(path_or_file=response.contents)


if __name__ == "__main__":
    port = int(os.environ.get('DATABRICKS_APP_PORT', app.config["DATABRICKS_APP_PORT"]))
    app.run(debug=True, host='0.0.0.0', port=port)