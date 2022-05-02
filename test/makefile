.PHONY: nothing
nothing:
	@echo What are you doing?

.PHONY: end-to-end
end-to-end:
	@docker-compose down --remove-orphans
	@docker-compose build --build-arg=GITHUB_USERNAME=$(GITHUB_USERNAME) --build-arg=GITHUB_TOKEN=$(GITHUB_TOKEN) --no-cache
	@docker-compose run test
	@docker-compose down --remove-orphans
