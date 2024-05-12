# DAB Lakehouse Boilerplate

Current version: 0.1.0

## Prerequisites

* Python
* pyenv
* poetry
* Databricks workspace and cluster details - URL and token

1. Install python version 3.10.12 using pyenv.
    ```
    $ pyenv install 3.10.12
    ```
2. Install the Databricks CLI https://docs.databricks.com/dev-tools/cli/databricks-cli.html
    ```
    $ curl -fsSL https://raw.githubusercontent.com/databricks/setup-cli/main/install.sh
    ```

3. Authenticate to your Databricks workspace, if you have not done so already:
    ```
    $ databricks configure
    ```
4. Install databrikcs extension - https://marketplace.visualstudio.com/items?itemName=databricks.databricks and configure.
5. Make sure you provide cluster_id in .databrickscfg (The 'databricks configure' might not be asking for cluster_id but the same is required for running and debugging files locally.)
6. Set local python version as 3.10.12.
    ```
    $ pyenv local 3.10.12
    ```
7. Create venv using poetry.
    ```
    $ poetry shell
    ```
8. Install libraries
    ```
    $ poetry install
    ```
9. Update databricks.yml with your own cluster id.

10. To deploy a development copy of this project, type:
    ```
    $ databricks bundle deploy --target dev
    ```
    (Note that "dev" is the default target, so the `--target` parameter
    is optional here.)

    This deploys everything that's defined for this project.
    For example, the default template would deploy a job called
    `[dev yourname] dab_boilerplate_job` to your workspace.
    You can find that job by opening your workpace and clicking on **Workflows**.

11. Similarly, to deploy a production copy, type:
   ```
   $ databricks bundle deploy --target prod
   ```

   Note that the default job from the template has a schedule that runs every day
   (defined in resources/dab_boilerplate_job.yml). The schedule
   is paused when deploying in development mode (see
   https://docs.databricks.com/dev-tools/bundles/deployment-modes.html).

12. To run a job or pipeline, use the "run" command:
   ```
   $ databricks bundle run
   ```

13. For documentation on the Databricks asset bundles format used
   for this project, and for CI/CD configuration, see
   https://docs.databricks.com/dev-tools/bundles/index.html.

14. To configure github repo updates HOST and TOKEN secrets.

15. If you are contributing perform pre-commit, pytest and punch before merge.
   ```
   $ punch --part patch  # For a bug fix
   $ punch --part minor  # For new features
   $ punch --part major  # For major changes

   ```
