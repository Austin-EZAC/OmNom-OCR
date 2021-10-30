echo "Copying lambda functions..."

Copy ..\src\helper.py lambda\helper\python\helper.py
Copy ..\src\datastore.py lambda\helper\python\datastore.py
Copy ..\src\s3proc.py lambda\s3processor\lambda_function.py
Copy ..\src\s3batchproc.py lambda\s3batchprocessor\lambda_function.py
Copy ..\src\docproc.py lambda\documentprocessor\lambda_function.py
Copy ..\src\syncproc.py lambda\syncprocessor\lambda_function.py
Copy ..\src\asyncproc.py lambda\asyncprocessor\lambda_function.py
Copy ..\src\jobresultsproc.py lambda\jobresultprocessor\lambda_function.py
Copy ..\src\trp.py lambda\textractor\python\trp.py
Copy ..\src\og.py lambda\textractor\python\og.py

echo "Done!"
