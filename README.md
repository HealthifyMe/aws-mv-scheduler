# MV Scheduler

A Python-based scheduling system that integrates with AWS Redshift and Slack for automated data processing and notifications.

## Overview

MV Scheduler is a tool designed to automate the scheduling refreshes of materialized views (MVs) in Redshift, with built-in Slack notifications for process monitoring.

## Features

- Automated scheduling of materialized view refreshes
- AWS Redshift integration
- Slack notifications for failed MV refreshes

## Prerequisites

- Docker
- AWS Account with Redshift, DynamoDB access
- Slack workspace with bot integration

## Configuration

Create `.env` file in the root directory with the following parameters:

```env
AWS_ACCESS_KEY_ID=your-aws-access-key-id
AWS_SECRET_ACCESS_KEY=your-aws-secret-access-key
AWS_REGION=your-aws-region
REDSHIFT_HOST=your-redshift-host
REDSHIFT_PORT=your-redshift-port
REDSHIFT_DB_NAME=your-redshift-db-name
REDSHIFT_USER_NAME=your-redshift-user-name
REDSHIFT_PASSWORD=your-redshift-password
SLACK_BOT_TOKEN=your-slack-bot-token
SLACK_CHANNEL_ID=your-slack-channel-id
```

Update `mvs_config.yaml` file in the root directory with the following format:
```yaml
mvs:
  - name: mv_name_1
    frequency: "23 30 * * *"
    refresh_buffer: 10
  - name: mv_name_2
    frequency: "45 08 * * *"
    refresh_buffer: 5
```

## Installation

1. Clone the repository:
```bash
git clone https://github.com/mathewfrancis/mv-scheduler.git
cd mv-scheduler
```

## Usage
2. To build and run the scheduler using docker, execute the following command in your terminal:

```bash
./build_and_run.sh
```

## Project Structure

- `build_and_run.sh`: Shell script for building and running the docker image
- `scheduler.py`: Main scheduling logic
- `processor.py`: Materialized view processing implementation
- `slack_msg.py`: Slack notification handling
- `.env`: Environment variable configuration
- `requirements.txt`: Dependency requirements
- `Dockerfile`: Docker image configuration
- `mvs_config.yaml`: Materialized view configuration

## License
MIT License

## Contributing
To contribute to this project, follow these steps:

1. Fork the repository to your GitHub account.
2. Create a new branch for your feature or fix.
3. Make your changes, ensuring they align with the project's goals and coding standards.
4. Test your changes thoroughly to ensure they do not break existing functionality.
5. Submit a pull request to the original repository, including a detailed description of your changes.
6. Engage with the project maintainers and address any feedback or concerns they may have.
7. Once your pull request is approved, your changes will be merged into the project.

Remember to respect the project's license and adhere to the principles of open-source collaboration.