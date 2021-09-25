echo "Copying lambda functions..."
Copy helper.py ..\textract-pipeline\lambda\helper\python\helper.py
Copy datastore.py ..\textract-pipeline\lambda\helper\python\datastore.py
Copy s3proc.py ..\textract-pipeline\lambda\s3processor\lambda_function.py
Copy s3batchproc.py ..\textract-pipeline\lambda\s3batchprocessor\lambda_function.py
Copy docproc.py ..\textract-pipeline\lambda\documentprocessor\lambda_function.py
Copy syncproc.py ..\textract-pipeline\lambda\syncprocessor\lambda_function.py
Copy asyncproc.py ..\textract-pipeline\lambda\asyncprocessor\lambda_function.py
Copy jobresultsproc.py ..\textract-pipeline\lambda\jobresultprocessor\lambda_function.py

Copy trp.py ..\textract-pipeline\lambda\textractor\python\trp.py
Copy og.py ..\textract-pipeline\lambda\textractor\python\og.py

echo "Done!"
