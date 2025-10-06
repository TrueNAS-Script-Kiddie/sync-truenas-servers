SRC_DIR := /mnt/backup-pool/homedir-ds/home/admin/bin/sync_truenas_servers
DST_DIR := /mnt/backup-pool/homedir-ds/home/root/bin/sync_truenas_servers
BACKUP_DIR := /mnt/backup-pool/homedir-ds/home/root/bin/backup_$(shell date +%Y%m%d%H%M%S)

.PHONY: promote backup deploy rollback

promote: backup deploy

backup:
	@if [ -d "$(DST_DIR)" ]; then \
		echo "Backing up existing deployment to $(BACKUP_DIR)"; \
		cp -a $(DST_DIR) $(BACKUP_DIR); \
	else \
		echo "No existing deployment to back up"; \
	fi

deploy:
	@echo "Deploying from $(SRC_DIR) to $(DST_DIR)"
	mkdir -p $(DST_DIR)
	# Copy tracked files
	cd $(SRC_DIR) && git ls-files | xargs -I{} rsync -a {} $(DST_DIR)/
	# Also copy local config if present
	@if [ -f "$(SRC_DIR)/config.local.bash" ]; then \
		echo "Copying local config"; \
		rsync -a $(SRC_DIR)/config.local.bash $(DST_DIR)/; \
	fi

rollback:
	@if [ -d "$(BACKUP_DIR)" ]; then \
		echo "Rolling back to $(BACKUP_DIR)"; \
		rm -rf $(DST_DIR); \
		cp -a $(BACKUP_DIR) $(DST_DIR); \
	else \
		echo "No backup found to roll back to"; \
	fi