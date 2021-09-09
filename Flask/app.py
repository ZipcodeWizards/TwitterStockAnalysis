from flask import Flask, render_template, request


app = Flask(__name__)

@app.route('/', methods=['GET', 'POST'])
def main():
    dates = ['2021-09-07','2021-09-06','2021-09-05','2021-09-04','2021-09-03']
  
    return render_template("home.html", dates=dates)

if __name__ == "__main__":
    app.run()