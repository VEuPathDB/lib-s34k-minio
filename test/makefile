PHONY: nothing
nothing:

PHONY: end-to-end
end-to-end:
	@docker-compose down --remove-orphans
	@docker-compose build --build-arg=GITHUB_USERNAME=$(GITHUB_USERNAME) --build-arg=GITHUB_TOKEN=$(GITHUB_TOKEN)
	@docker-compose run test
	@docker-compose down --remove-orphans
