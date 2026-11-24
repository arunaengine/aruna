<script setup lang="ts">
import { onMounted, ref } from "vue";
import { LogIn, Search } from "@lucide/vue";
import { Button } from "@/components/ui/button";
import { Card, CardContent, CardDescription, CardHeader, CardTitle } from "@/components/ui/card";
import { Input } from "@/components/ui/input";
import { ArunaError, arunaJson, signIn } from "@/lib/aruna";

const session = ref<Record<string, unknown> | null>(null);
const signedOut = ref(false);
const query = ref("");

onMounted(async () => {
  try {
    session.value = await arunaJson<Record<string, unknown>>("session");
  } catch (error) {
    signedOut.value = error instanceof ArunaError && error.status === 401;
  }
});
</script>

<template>
  <main class="mx-auto flex max-w-2xl flex-col gap-6 p-6">
    <header data-aruna-part="intro" data-aruna-option="plain">
      <h1 class="text-2xl font-semibold">New applet</h1>
      <p class="text-muted-foreground">Replace this page with your applet.</p>
    </header>
    <header data-aruna-part="intro" data-aruna-option="bold" hidden>
      <h1 class="text-3xl font-bold tracking-tight">New applet</h1>
      <p class="text-muted-foreground">A second intro the creator can compare in the preview.</p>
    </header>

    <Card>
      <CardHeader>
        <CardTitle>Search</CardTitle>
        <CardDescription>A starting point built from the starter components.</CardDescription>
      </CardHeader>
      <CardContent class="flex gap-2">
        <Input v-model="query" placeholder="Search term" />
        <Button><Search />Search</Button>
      </CardContent>
    </Card>

    <Button v-if="signedOut" variant="outline" class="self-start" @click="signIn()">
      <LogIn />Sign in
    </Button>
    <pre v-else-if="session" class="rounded-md bg-muted p-3 text-xs">{{ session }}</pre>
  </main>
</template>
