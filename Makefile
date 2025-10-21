SOURCE_DIR := /mnt/backup-pool/homedir-ds/home/admin/bin/sync_truenas_servers
TARGET_DIR := /mnt/backup-pool/homedir-ds/home/root/bin/sync_truenas_servers
BACKUP_DIR := /mnt/backup-pool/homedir-ds/home/root/bin/backup_$(shell date +%Y%m%d%H%M%S)

.PHONY: promote backup deploy rollback

promote: backup deploy

backup:
	@if [ -d "$(TARGET_DIR)" ]; then \
		echo "Backing up existing deployment to $(BACKUP_DIR)"; \
		cp -a $(TARGET_DIR) $(BACKUP_DIR); \
	else \
		echo "No existing deployment to back up"; \
	fi

deploy:
	@echo "Deploying from $(SOURCE_DIR) to $(TARGET_DIR)"
	mkdir -p $(TARGET_DIR)
	# Copy tracked files
	cd $(SOURCE_DIR) && git ls-files | xargs -I{} rsync -a {} $(TARGET_DIR)/
	# Also copy local config if present
	@if [ -f "$(SOURCE_DIR)/config.local.bash" ]; then \
		echo "Copying local config"; \
		rsync -a $(SOURCE_DIR)/config.local.bash $(TARGET_DIR)/; \
	fi

rollback:
	@if [ -d "$(BACKUP_DIR)" ]; then \
		echo "Rolling back to $(BACKUP_DIR)"; \
		rm -rf $(TARGET_DIR); \
		cp -a $(BACKUP_DIR) $(TARGET_DIR); \
	else \
		echo "No backup found to roll back to"; \
	fi