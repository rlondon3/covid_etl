from models.covid import Covid_Store

store = Covid_Store()

class Covid_Handler():
    # Get covid data
    def get_data():
        try:
            data = store.index()
            if data:
                return data
        except Exception as e:
            return {"Error": str(e)}, 404
        
    # Get covid data row
    def get_data_row(id):
        row_id = id
        try:
            row = store.show(row_id)
            if row:
                return row
        except Exception as e:
            return {"Error": str(e)}

# create routes to access data
def covid_routes(app):
    app.add_url_rule("/covid_data", "show_data", Covid_Handler.get_data, methods=["POST"])
    app.add_url_rule("/covid_data/<id>", "show_row", Covid_Handler.get_data_row, methods=["POST"])